use std::{
    collections::HashSet,
    error::Error,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

use incrementalmerkletree::{Position, Retention};
use orchard::{
    Anchor, Note,
    builder::{Builder as OrchardBuilder, BundleType},
    bundle::BundleVersion,
    circuit::ProvingKey,
    keys::{FullViewingKey, Scope, SpendAuthorizingKey, SpendingKey},
    tree::{MerkleHashOrchard, MerklePath},
    value::NoteValue,
};
use rand::{TryRng, rand_core::UnwrapErr, rngs::SysRng};
use rayon::prelude::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shardtree::{ShardTree, store::memory::MemoryShardStore};
use zcash_keys::keys::{UnifiedAddressRequest, UnifiedSpendingKey};
use zcash_primitives::transaction::{
    Authorized, Transaction, TransactionData, TxVersion, Unauthorized,
    sighash::{PrecomputedSighashData, SignableInput},
};
use zcash_protocol::{
    consensus::{BlockHeight, BranchId},
    local_consensus::LocalNetwork,
    value::ZatBalance,
};
use zip32::{AccountId, DiversifierIndex};

type Failure = Box<dyn Error + Send + Sync>;
type Result<T, E = Failure> = std::result::Result<T, E>;
type Tree = ShardTree<MemoryShardStore<MerkleHashOrchard, u32>, 32, 16>;

fn argument<'a>(args: &'a [String], flag: &str) -> Result<&'a str> {
    let mut values = args.windows(2).filter(|pair| pair[0] == flag);
    let found = values.next().ok_or_else(|| format!("missing {flag}"))?;
    if values.next().is_some() || found[1].starts_with("--") {
        return Err(format!("invalid {flag}").into());
    }
    Ok(&found[1])
}

fn count(args: &[String]) -> Result<usize> {
    let count = argument(args, "--count")?.parse::<usize>()?;
    if count == 0 || count > 330 {
        return Err("count must be between 1 and 330".into());
    }
    Ok(count)
}

fn network() -> LocalNetwork {
    let activation = Some(BlockHeight::from_u32(1));
    LocalNetwork {
        overwinter: activation,
        sapling: activation,
        blossom: activation,
        heartwood: activation,
        canopy: activation,
        nu5: activation,
        nu6: None,
        nu6_1: None,
        nu6_2: None,
        nu6_3: None,
        nu7: None,
    }
}

fn seed(path: &Path, create: bool) -> Result<[u8; 32]> {
    if create && !path.exists() {
        let mut seed = [0; 32];
        SysRng.try_fill_bytes(&mut seed)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(&seed)?;
        file.sync_all()?;
        return Ok(seed);
    }
    let bytes = fs::read(path)?;
    if bytes.len() != 32 {
        return Err("seed file must contain exactly 32 bytes".into());
    }
    let mut seed = [0; 32];
    seed.copy_from_slice(&bytes);
    Ok(seed)
}

fn addresses(args: &[String]) -> Result<()> {
    let seed = seed(Path::new(argument(args, "--seed-file")?), true)?;
    let params = network();
    let usk = UnifiedSpendingKey::from_seed(&params, &seed, AccountId::ZERO)?;
    let ufvk = usk.to_unified_full_viewing_key();
    let mut index = DiversifierIndex::new();
    for _ in 0..count(args)? {
        let (address, found) = ufvk.find_address(index, UnifiedAddressRequest::ORCHARD)?;
        println!("{}", address.encode(&params));
        index = found;
        index.increment().map_err(|_| "diversifier indices exhausted")?;
    }
    Ok(())
}

fn one_tx(
    sk: &SpendingKey,
    fvk: &FullViewingKey,
    note: Note,
    path: MerklePath,
    anchor: Anchor,
    height: u32,
    fee: u64,
    destination: u32,
    pk: &ProvingKey,
) -> Result<(Value, [u8; 32])> {
    let mut rng = UnwrapErr(SysRng);
    let version = BundleVersion::orchard_insecure_v1();
    let mut builder = OrchardBuilder::new(
        BundleType::UNPADDED,
        version,
        version.default_flags(),
        anchor,
    )?;
    let output = note.value().inner().checked_sub(fee).ok_or("funding too small")?;
    if output == 0 {
        return Err("output must have positive value".into());
    }
    builder.add_spend(fvk.clone(), note, path)?;
    builder.add_output(
        None,
        fvk.address_at(destination, Scope::External),
        NoteValue::from_raw(output),
        [0; 512],
    )?;
    let (bundle, _) = builder.build::<ZatBalance>(&mut rng)?.ok_or("missing Orchard bundle")?;
    if bundle.actions().len() != 1 {
        return Err("expected exactly one Orchard action".into());
    }
    let pre = PrecomputedSighashData::new_zip244(TransactionData::<Unauthorized>::from_parts(
        TxVersion::V5,
        BranchId::Nu5,
        0,
        BlockHeight::from_u32(height.checked_add(40).ok_or("expiry height overflow")?),
        None,
        None,
        None,
        Some(bundle),
    ))
    .map_err(|_| "V5 required")?;
    let sighash = *pre.signature_hash(&SignableInput::Shielded).as_ref();
    let ask = SpendAuthorizingKey::from(sk);
    let signed: TransactionData<Authorized> = pre.into_tx().try_map_bundles(
        |_| Ok::<_, Failure>(None),
        |_| Ok::<_, Failure>(None),
        |bundle| {
            bundle
                .map(|bundle| -> Result<_> {
                    let proven = bundle.create_proof(pk, &mut rng)?;
                    Ok(proven.apply_signatures(&mut rng, sighash, std::slice::from_ref(&ask))?)
                })
                .transpose()
        },
    )?;
    let tx = signed.freeze()?;
    let nullifier = tx.orchard_bundle().ok_or("missing Orchard bundle")?
        .actions()[0].nullifier().to_bytes();
    if tx.transparent_bundle().is_some()
        || tx.sapling_bundle().is_some()
        || tx.sprout_bundle().is_some()
        || tx.orchard_bundle().ok_or("missing Orchard bundle")?.actions().len() != 1
    {
        return Err("transaction is not pure one-action Orchard".into());
    }
    let mut raw = Vec::new();
    tx.write(&mut raw)?;
    if raw.len() > 2_000_000 {
        return Err("transaction exceeds block limit".into());
    }
    Ok((json!({
        "txid": tx.txid().to_string(),
        "raw": hex::encode(&raw),
        "kind": "orchard",
        "bytes": raw.len(),
        "sha256": hex::encode(Sha256::digest(&raw)),
        "orchard_actions": 1,
        "fee_zat": fee,
    }), nullifier))
}

fn spend(args: &[String]) -> Result<()> {
    let expected = count(args)?;
    let fee: u64 = argument(args, "--fee")?.parse()?;
    if fee != 20_000 {
        return Err("fixture fee must be 20000 zatoshis".into());
    }
    let seed = seed(Path::new(argument(args, "--seed-file")?), false)?;
    let usk = UnifiedSpendingKey::from_seed(&network(), &seed, AccountId::ZERO)?;
    let sk = usk.orchard();
    let fvk = FullViewingKey::from(sk);
    let ivk = fvk.to_ivk(Scope::External);
    let mut tree = Tree::new(MemoryShardStore::empty(), 1);
    let mut owned: Vec<(Note, Position)> = Vec::new();
    let file = File::open(argument(args, "--blocks")?)?;
    let mut tip = 0u32;
    let mut rpc_root = String::new();
    for line in BufReader::new(file).lines() {
        let block: Value = serde_json::from_str(&line?)?;
        let height = u32::try_from(block["height"].as_u64().ok_or("block missing height")?)?;
        if height != tip.checked_add(1).ok_or("setup height overflow")? {
            return Err(format!("missing or reordered block at height {height}").into());
        }
        tip = height;
        rpc_root = block["finalorchardroot"]
            .as_str()
            .ok_or("block missing finalorchardroot")?
            .to_owned();
        for encoded in block["tx"].as_array().ok_or("block missing transactions")? {
            let raw = hex::decode(encoded["hex"].as_str().ok_or("transaction missing hex")?)?;
            let tx = Transaction::read(raw.as_slice(), BranchId::Nu5)?;
            if let Some(bundle) = tx.orchard_bundle() {
                for (index, action) in bundle.actions().iter().enumerate() {
                    let decrypted = bundle.decrypt_output_with_key(index, &ivk);
                    let owned_note = decrypted.filter(|(note, _, _)| {
                        let commitment: orchard::note::ExtractedNoteCommitment = note.commitment().into();
                        &commitment == action.cmx()
                    });
                    let marked = owned_note.is_some();
                    tree.append(
                        MerkleHashOrchard::from_cmx(action.cmx()),
                        if marked { Retention::Marked } else { Retention::Ephemeral },
                    )?;
                    if let Some((note, _, _)) = owned_note {
                        if note.value().inner() != 2_000_000 {
                            return Err("owned funding note does not contain 0.02 ZEC".into());
                        }
                        let position = tree.max_leaf_position(None)?.ok_or("missing appended leaf")?;
                        owned.push((note, position));
                    }
                }
            }
        }
    }
    if tip == 0 || owned.len() != expected {
        return Err(format!("found {} owned notes, expected {expected}", owned.len()).into());
    }
    tree.checkpoint(tip)?;
    let root = tree.root_at_checkpoint_id(&tip)?.ok_or("missing tip checkpoint")?;
    let anchor: Anchor = root.into();
    // zcashd renders `finalorchardroot` as the canonical bytes (`HexStr`), unlike its `GetHex` roots.
    if hex::decode(&rpc_root)? != root.to_bytes() {
        return Err(format!(
            "computed Orchard root {} differs from finalorchardroot {rpc_root}",
            hex::encode(root.to_bytes())
        )
        .into());
    }
    let mut spendable = Vec::with_capacity(expected);
    let mut positions = HashSet::with_capacity(expected);
    for (note, position) in owned {
        if !positions.insert(position) {
            return Err("duplicate funding commitment position".into());
        }
        let witness = tree
            .witness_at_checkpoint_id(position, &tip)?
            .ok_or("missing tip witness")?;
        let path: MerklePath = witness.into();
        if path.root(note.commitment().into()) != anchor {
            return Err("witness does not match tip anchor".into());
        }
        spendable.push((note, path));
    }
    let pk = ProvingKey::build(BundleVersion::orchard_insecure_v1().circuit_version());
    let generated: Vec<(Value, [u8; 32])> = spendable
        .into_par_iter()
        .enumerate()
        .map(|(index, (note, path))| {
            let destination = u32::try_from(expected.checked_add(index).ok_or("index overflow")?)?;
            one_tx(sk, &fvk, note, path, anchor, tip, fee, destination, &pk)
        })
        .collect::<Result<_>>()?;
    let ids: HashSet<_> = generated.iter().map(|(tx, _)| tx["txid"].as_str()).collect();
    let nullifiers: HashSet<_> = generated.iter().map(|(_, nf)| nf).collect();
    if ids.len() != expected || nullifiers.len() != expected {
        return Err("duplicate transaction IDs or nullifiers".into());
    }
    let corpus: Vec<_> = generated.into_iter().map(|(tx, _)| tx).collect();
    let out = Path::new(argument(args, "--out")?);
    let mut file = File::create(out)?;
    serde_json::to_writer_pretty(&mut file, &corpus)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("addresses") => addresses(&args[1..]),
        Some("spend") => spend(&args[1..]),
        _ => Err("usage: orchard-producer addresses|spend --seed-file PATH --count N [--blocks BLOCKS --fee 20000 --out CORPUS]".into()),
    }
}
