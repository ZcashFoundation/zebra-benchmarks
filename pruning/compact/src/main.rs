use rocksdb::{BlockBasedOptions, BottommostLevelCompaction, ColumnFamilyDescriptor, CompactOptions,
              DBCompressionType, MergeOperands, Options, DB};
use std::{error::Error, path::Path, time::Instant};

// Zebra v29/v30 disk_format/transparent.rs: i64 LE balance, 8-byte ordered
// output location, optional u64 LE received. This is the only custom merge CF.
fn merge_balance(_: &[u8], existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut balance = 0_i64;
    let mut location = [255_u8; 8];
    let mut received = 0_u64;
    for value in existing.into_iter().chain(operands) {
        if value.len() != 16 && value.len() != 24 { return None; }
        let change = i64::from_le_bytes(value[..8].try_into().ok()?);
        if !(-2_100_000_000_000_000..=2_100_000_000_000_000).contains(&change) { return None; }
        balance = balance.checked_add(change)?;
        if !(-2_100_000_000_000_000..=2_100_000_000_000_000).contains(&balance) { return None; }
        location = location.min(value[8..16].try_into().ok()?);
        if value.len() == 24 {
            received = received.saturating_add(u64::from_le_bytes(value[16..].try_into().ok()?));
        }
    }
    let mut result = Vec::with_capacity(24);
    result.extend_from_slice(&balance.to_le_bytes());
    result.extend_from_slice(&location);
    result.extend_from_slice(&received.to_le_bytes());
    Some(result)
}

fn options() -> Options {
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::Lz4);
    options.optimize_level_style_compaction(128 * 1024 * 1024);
    options.set_max_background_jobs(4);
    options.set_max_open_files(512);
    let mut table = BlockBasedOptions::default();
    table.set_ribbon_filter(9.9);
    options.set_block_based_table_factory(&table);
    options
}

fn descriptors(names: &[String]) -> Vec<ColumnFamilyDescriptor> {
    names.iter().map(|name| {
        let mut opts = options();
        if name == "balance_by_transparent_addr" {
            opts.set_merge_operator_associative("fetch_add_balance_and_received", merge_balance);
        }
        ColumnFamilyDescriptor::new(name, opts)
    }).collect()
}

fn compact(path: &Path) -> Result<(), Box<dyn Error>> {
    let opts = options();
    let names = DB::list_cf(&opts, path)?;
    let db = DB::open_cf_descriptors(&opts, path, descriptors(&names))?;
    let mut compact = CompactOptions::default();
    // Plain compact_range can skip the bottommost level and leave tombstones.
    compact.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
    for name in &names {
        let cf = db.cf_handle(name).ok_or("listed CF missing after open")?;
        let start = Instant::now();
        db.flush_cf(cf)?;
        let before = db.property_int_value_cf(cf, "rocksdb.total-sst-files-size")?.unwrap_or(0);
        db.compact_range_cf_opt::<&[u8], &[u8]>(cf, None, None, &compact);
        db.flush_cf(cf)?;
        let errors = db.property_int_value("rocksdb.background-errors")?.ok_or("missing RocksDB error counter")?;
        if errors != 0 { return Err(format!("RocksDB reported {errors} errors; inspect LOG").into()); }
        let after = db.property_int_value_cf(cf, "rocksdb.total-sst-files-size")?.unwrap_or(0);
        // Known Zebra column-family names contain no JSON metacharacters.
        if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err("unsupported column-family name".into());
        }
        println!("{{\"cf\":\"{name}\",\"before_sst_bytes\":{before},\"after_sst_bytes\":{after},\"seconds\":{}}}", start.elapsed().as_secs_f64());
    }
    db.flush()?;
    Ok(())
}

fn smoke(path: &Path) -> Result<(), Box<dyn Error>> {
    if path.exists() { return Err("smoke path must not exist".into()); }
    let names = vec!["default".into(), "tx_by_loc".into(), "balance_by_transparent_addr".into()];
    let mut opts = options();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    {
        let db = DB::open_cf_descriptors(&opts, path, descriptors(&names))?;
        let tx = db.cf_handle("tx_by_loc").ok_or("missing smoke tx CF")?;
        for n in 0_u32..1000 { db.put_cf(tx, n.to_be_bytes(), [7_u8; 1024])?; }
        db.flush_cf(tx)?;
        db.delete_range_cf(tx, 0_u32.to_be_bytes(), 900_u32.to_be_bytes())?;
        let balance = db.cf_handle("balance_by_transparent_addr").ok_or("missing smoke balance CF")?;
        let mut old = 10_i64.to_le_bytes().to_vec();
        old.extend_from_slice(&[2; 8]);
        db.put_cf(balance, b"address", &old)?;
        let mut change = (-3_i64).to_le_bytes().to_vec();
        change.extend_from_slice(&[1; 8]);
        change.extend_from_slice(&5_u64.to_le_bytes());
        db.merge_cf(balance, b"address", &change)?;
    }
    compact(path)?;
    let db = DB::open_cf_descriptors(&options(), path, descriptors(&names))?;
    let tx = db.cf_handle("tx_by_loc").ok_or("missing smoke tx CF")?;
    assert!(db.get_cf(tx, 0_u32.to_be_bytes())?.is_none());
    assert!(db.get_cf(tx, 999_u32.to_be_bytes())?.is_some());
    let balance = db.cf_handle("balance_by_transparent_addr").ok_or("missing smoke balance CF")?;
    let value = db.get_cf(balance, b"address")?.ok_or("missing merged smoke balance")?;
    assert_eq!(&value[..8], &7_i64.to_le_bytes());
    assert_eq!(&value[8..16], &[1; 8]);
    assert_eq!(&value[16..], &5_u64.to_le_bytes());
    eprintln!("compactor smoke: PASS (range deletion, all CFs, Zebra balance merge)");
    Ok(())
}

/// Restrict mutation to an explicitly selected directory, including through ancestor symlinks.
fn checked_path(root: &Path, path: &Path) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let root = root.canonicalize()?;
    let parent = path.parent().ok_or("database needs a parent")?.canonicalize()?;
    if !parent.starts_with(&root) || path.is_symlink() {
        return Err("refusing databases outside --allowed-root or symlinks".into());
    }
    let checked = if path.exists() {
        path.canonicalize()?
    } else {
        parent.join(path.file_name().ok_or("database needs a file name")?)
    };
    if checked == root || !checked.starts_with(&root) {
        return Err("database must be strictly inside --allowed-root".into());
    }
    Ok(checked)
}

fn main() -> Result<(), Box<dyn Error>> {
    const USAGE: &str = "usage: prune-compact --allowed-root ROOT [--smoke] DATABASE";
    let args: Vec<_> = std::env::args().collect();
    if args.len() == 1 || args[1..].iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{USAGE}");
        return Ok(());
    }
    let (root, mode, path) = match args.as_slice() {
        [_, flag, root, path] if flag == "--allowed-root" => (root, "compact", path),
        [_, flag, root, mode, path] if flag == "--allowed-root" && mode == "--smoke" =>
            (root, mode.as_str(), path),
        _ => return Err(USAGE.into()),
    };
    let path = checked_path(Path::new(root), Path::new(path))?;
    if mode == "--smoke" { smoke(&path) } else { compact(&path) }
}
