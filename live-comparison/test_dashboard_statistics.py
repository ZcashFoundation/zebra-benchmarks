"""Synthetic comparison, chart and public-export contracts for the dashboard."""
import csv
import io
import json
import re
import shutil
import subprocess
import tempfile
import unittest

import dashboard
import test_dispatch


class DashboardStatisticsTests(unittest.TestCase):
    def test_public_block_export_and_csv(self):
        helper = test_dispatch.DashboardTests()
        rows = helper.records(3)
        rows = [row for row in rows if not (row.get("height") == 2 and row.get("target") == "zebra-reference")]
        rows.append({"event": "ignored_metadata", "detail": "/path/to/OMITTED_LOG"})
        with tempfile.TemporaryDirectory() as directory:
            path = helper.write_records(directory, rows)
            data = dashboard.collect(path)
        self.assertEqual(len(data["per_block"]), 3)
        for row in data["per_block"]:
            self.assertEqual(set(row), {"height", "hash", "size_bytes", "busy", "latency_ms"})
            self.assertEqual(set(row["latency_ms"]), set(dashboard.TARGETS))
        exported = {row["height"]: row for row in data["per_block"]}
        self.assertEqual(exported[0]["hash"], "0" * 64)
        self.assertEqual(exported[0]["latency_ms"]["zebra-candidate"], 5)
        self.assertIsNone(exported[2]["latency_ms"]["zebra-reference"])
        html = dashboard.render(data)
        embedded = html.split('<script id="dashboard-data" type="application/json">')[1].split("</script>")[0]
        self.assertEqual(json.loads(embedded)["per_block"], data["per_block"])
        if shutil.which("node") is None:
            self.skipTest("CSV browser function additionally checked when Node.js is installed")
        javascript = re.findall(r"<script>(.*?)</script>", html, flags=re.S)[-1]
        setup = "const window={}; const document={getElementById:()=>({textContent: " + json.dumps(embedded) + ",addEventListener:()=>{}})};\n"
        result = subprocess.run(["node", "-e", setup + javascript + "\nconsole.log(JSON.stringify(perBlockCSV(d)));"],
                                text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        table = list(csv.reader(io.StringIO(json.loads(result.stdout))))
        self.assertEqual(table[0], ["height", "hash", "size_bytes", "optimized_zebra_ms", "zebra_v7_ms", "zakura_ms", "host_busy"])
        self.assertEqual(table[1], ["0", "0" * 64, "10000", "5", "12", "2", ""])
        self.assertEqual(table[3][4], "")

    def test_rejected_first_attempt_has_no_acceptance_latency(self):
        helper = test_dispatch.DashboardTests()
        rows = helper.records(2)
        rows[-1]["outcome"] = "rejected"
        with tempfile.TemporaryDirectory() as directory:
            data = dashboard.collect(helper.write_records(directory, rows))
        self.assertIsNone(data["per_block"][-1]["latency_ms"]["zebra-candidate"])
        glance = data["glance"]
        self.assertIsNone(glance["recent"][0]["latency_ms"]["zebra-candidate"])
        self.assertEqual([point[0] for point in glance["points"]], [0])
        for node in glance["nodes"].values():
            self.assertEqual(node["n"], 1)
        self.assertEqual(glance["comparisons"]["zebra-reference"],
                         {"n": 1, "ratio": 2.4, "saving_ms": 7, "saving_n": 1})
        self.assertIn(
            f'<tr><th scope="row" title="{rows[-1]["hash"]}">1</th>'
            '<td>10.000</td><td>—</td><td>12.0</td><td>2.0</td><td>unknown</td></tr>',
            dashboard.render(data))

    def test_glance_window_uses_first_dispatch_and_keeps_mixed_load(self):
        helper = test_dispatch.DashboardTests()
        rows = helper.records(6)
        day = 24 * 60 * 60 * 1000
        offsets = (-1, 0, 300_000, 600_000, 900_000, day)
        for row in rows[1:]:
            row["queued_unix_ms"] = row["dispatch_unix_ms"] = helper.BASE + offsets[row["height"]]
            row["observed_at_unix_ms"] = row["dispatch_unix_ms"] + 20
            if row["height"] == 0:
                row["latency_ms"] = 999
            if row["height"] == 3 and row["target"] == "zebra-candidate":
                row["outcome"] = "rejected"
                retry = dict(row, outcome="accepted", dispatch_unix_ms=helper.BASE + 3 * day,
                             observed_at_unix_ms=helper.BASE + 3 * day + 20)
        rows = [row for row in rows if not (row.get("height") == 4 and row.get("target") == "zebra-reference")]
        rows.extend((retry, dict(rows[0], at_unix_ms=helper.BASE + 4 * day)))
        with tempfile.TemporaryDirectory() as directory:
            path = helper.write_records(directory, list(reversed(rows)))
            data = dashboard.collect(path, load_intervals=[[helper.BASE - 1, helper.BASE + day]])
        glance = data["glance"]
        self.assertEqual((glance["window_start"], glance["window_end"]),
                         (dashboard.utc(helper.BASE), dashboard.utc(helper.BASE + day)))
        self.assertEqual([point[0] for point in glance["points"]], [1, 2, 5])
        for target, median in zip(dashboard.TARGETS, (2, 12, 5)):
            self.assertEqual(glance["nodes"][target], {"n": 3, "median_ms": median})
        self.assertEqual(glance["comparisons"]["zebra-reference"], {"n": 3, "ratio": 2.4, "saving_ms": 7, "saving_n": 3})
        self.assertEqual(glance["comparisons"]["zakura"], {"n": 3, "ratio": .4, "saving_ms": -3, "saving_n": 3})
        self.assertEqual(data["mixed_load"]["All sizes"]["n"], 4)
        html = dashboard.render(data)
        self.assertIn("2.40× faster", html)
        self.assertIn("2.50× slower", html)
        self.assertIn("Saves 7.0 ms per block", html)
        self.assertIn("Adds 3.0 ms per block", html)

    def test_glance_ratios_are_same_block_and_omit_zero_denominators(self):
        helper = test_dispatch.DashboardTests()
        rows = helper.records(4)
        for row in rows[1:]:
            i = row["height"]
            row["latency_ms"] = {
                "zakura": (2, 200, 202, 999),
                "zebra-reference": (99, 100, 102, 999),
                "zebra-candidate": (1, 100, 101, 0),
            }[row["target"]][i]
        with tempfile.TemporaryDirectory() as directory:
            path = helper.write_records(directory, rows)
            glance = dashboard.collect(path)["glance"]
            self.assertEqual(glance["nodes"]["zebra-candidate"]["n"], 4)
            self.assertEqual(glance["comparisons"]["zebra-reference"],
                             {"n": 3, "ratio": 102 / 101, "saving_ms": 49.5, "saving_n": 4})
            self.assertEqual(glance["comparisons"]["zakura"],
                             {"n": 3, "ratio": 2, "saving_ms": 100.5, "saving_n": 4})
            marginal_ratio = glance["nodes"]["zebra-reference"]["median_ms"] / glance["nodes"]["zebra-candidate"]["median_ms"]
            self.assertNotEqual(glance["comparisons"]["zebra-reference"]["ratio"], marginal_ratio)
            for row in rows[1:]:
                if row["target"] == "zebra-candidate":
                    row["latency_ms"] = 0
            data = dashboard.collect(helper.write_records(directory, rows))
        self.assertEqual(data["glance"]["comparisons"]["zebra-reference"],
                         {"n": 0, "ratio": None, "saving_ms": 101, "saving_n": 4})

    def test_glance_recent_blocks_tiny_sizes_and_empty_rendering(self):
        helper = test_dispatch.DashboardTests()
        for network in dashboard.NETWORKS:
            for count in (0, 12):
                with self.subTest(network=network, count=count), tempfile.TemporaryDirectory() as directory:
                    rows = helper.records(count)
                    for row in rows[1:]:
                        # Heights need not increase in observation order.
                        row["height"] = count - row["height"]
                        row["block_bytes"] = 123
                    rows = [row for row in rows if not (row.get("height") == 1 and row.get("target") == "zebra-reference")]
                    data = dashboard.collect(helper.write_records(directory, rows), network=network)
                    glance = data["glance"]
                    html = dashboard.render(data)
                    self.assertNotIn('href="#glance"', html)
                    if count:
                        self.assertEqual([row["height"] for row in glance["recent"]], list(range(1, 11)))
                        self.assertIsNone(glance["recent"][0]["latency_ms"]["zebra-reference"])
                        self.assertEqual(glance["nodes"]["zebra-candidate"]["n"], 11)
                        self.assertIn("<td>0.123</td>", html)
                        self.assertEqual(data["mixed_load"]["<20 kB"]["n"], 11)
                    else:
                        self.assertEqual(glance["recent"], [])
                        self.assertEqual(glance["points"], [])
                        for node in glance["nodes"].values():
                            self.assertEqual(node, {"n": 0, "median_ms": None})
                        self.assertEqual(glance["window_end"], "No observations")

    def test_glance_chart_traces_and_host_load_shading(self):
        if shutil.which("node") is None:
            self.skipTest("Plotly arguments additionally checked when Node.js is installed")
        helper = test_dispatch.DashboardTests()
        for count in (0, 3):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as directory:
                rows = helper.records(count)
                if count:
                    rows[-1]["latency_ms"] = 0
                data = dashboard.collect(helper.write_records(directory, rows),
                                         load_intervals=[[helper.BASE, helper.BASE + 600_000]])
                html = dashboard.render(data)
                javascript = re.findall(r"<script>(.*?)</script>", html, flags=re.S)[-1]
                setup = """
const plots={};
const Plotly={react:(id,traces,layout)=>{plots[id]={traces,layout};return Promise.resolve();}};
const window={Plotly,matchMedia:()=>({matches:false,addEventListener:()=>{}})};
const document={getElementById:()=>({textContent:DATA,addEventListener:()=>{}})};
""".replace("DATA", json.dumps(json.dumps(data)))
                result = subprocess.run(["node", "-e", setup + javascript + "\nconsole.log(JSON.stringify(plots));"],
                                        text=True, capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                plots = json.loads(result.stdout)
                self.assertEqual(set(plots), {"glance-chart", "size-chart"})
                chart = plots["glance-chart"]
                self.assertEqual(chart["layout"]["yaxis"]["type"], "log")
                self.assertEqual(chart["layout"]["xaxis"]["type"], "date")
                self.assertEqual([trace["name"] for trace in chart["traces"]], list(reversed(dashboard.LABELS)) * 2)
                self.assertEqual([len(trace["y"]) for trace in chart["traces"][:3]],
                                 [max(0, count - 1), count, count])
                for trace in chart["traces"][:3]:
                    self.assertEqual(trace["mode"], "markers")
                    self.assertLess(trace["marker"]["opacity"], .5)
                for trace in chart["traces"][3:]:
                    self.assertEqual(trace["mode"], "lines")
                    self.assertFalse(trace["connectgaps"])
                self.assertEqual(len(chart["layout"]["shapes"]), len(data["load_intervals"]))
                if count:
                    self.assertEqual(chart["layout"]["xaxis"]["range"],
                                     [data["glance"]["chart_start"], data["glance"]["window_end"]])
                    self.assertEqual(chart["traces"][0]["y"], [5, 5])
                    self.assertEqual(chart["traces"][0]["customdata"], [[0, 10], [1, 10]])
                    self.assertEqual(chart["layout"]["shapes"][0]["yref"], "paper")
                    self.assertEqual(data["glance"]["chart_start"], dashboard.utc(helper.BASE))
                    self.assertEqual(chart["traces"][3]["y"], [5, 5, 5])
                    self.assertEqual(chart["traces"][5]["y"], [2, 2, 2])
                else:
                    self.assertNotIn("range", chart["layout"]["xaxis"])

    def test_chart_themes_update_without_changing_observations(self):
        if shutil.which("node") is None:
            self.skipTest("Plotly theme updates additionally checked when Node.js is installed")
        helper = test_dispatch.DashboardTests()
        with tempfile.TemporaryDirectory() as directory:
            data = dashboard.collect(helper.write_records(directory, helper.records(3)),
                                     load_intervals=[[helper.BASE, helper.BASE]])
        javascript = re.findall(r"<script>(.*?)</script>", dashboard.render(data), flags=re.S)[-1]
        for initially_dark in (False, True):
            with self.subTest(initially_dark=initially_dark):
                setup = """
const plots={};
const Plotly={react:(id,traces,layout)=>{plots[id]={traces,layout};return Promise.resolve();}};
let onThemeChange;
const scheme={matches:INITIAL,addEventListener:(event,fn)=>{
  if(event!=='change') throw new Error('unexpected media event');
  onThemeChange=fn;
}};
const window={Plotly,matchMedia:query=>{
  if(query!=='(prefers-color-scheme: dark)') throw new Error('unexpected media query');
  return scheme;
}};
const document={getElementById:()=>({textContent:DATA,addEventListener:()=>{}})};
""".replace("INITIAL", json.dumps(initially_dark)).replace("DATA", json.dumps(json.dumps(data)))
                exercise = """
const before=JSON.parse(JSON.stringify(plots));
onThemeChange({matches:!scheme.matches});
console.log(JSON.stringify([before,plots]));
"""
                result = subprocess.run(["node", "-e", setup + javascript + exercise],
                                        text=True, capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                snapshots = json.loads(result.stdout)
                for dark, plots in zip((initially_dark, not initially_dark), snapshots):
                    colors = list(dashboard.DARK_COLORS if dark else dashboard.COLORS)
                    self.assertEqual(set(plots), {"glance-chart", "size-chart"})
                    for chart in plots.values():
                        layout = chart["layout"]
                        self.assertEqual(layout["paper_bgcolor"], "#151a21" if dark else "#fff")
                        self.assertEqual(layout["plot_bgcolor"], layout["paper_bgcolor"])
                        self.assertEqual(layout["legend"]["font"]["color"], layout["font"]["color"])
                        self.assertEqual(layout["hoverlabel"]["font"]["color"], layout["font"]["color"])
                        self.assertNotEqual(layout["hoverlabel"]["bgcolor"], layout["font"]["color"])
                        if layout["yaxis"].get("type") == "log":
                            self.assertEqual(layout["yaxis"]["dtick"], 1)
                            self.assertEqual(layout["yaxis"]["minor"]["dtick"], "D1")
                            self.assertTrue(layout["yaxis"]["minor"]["showgrid"])
                        for shape in layout.get("shapes", []):
                            self.assertLessEqual(shape["opacity"], .1)
                    chart = plots["glance-chart"]
                    self.assertEqual([trace["marker"]["color"] for trace in chart["traces"][:3]], list(reversed(colors)))
                    line = chart["traces"][5]["line"]
                    self.assertEqual(line["dash"], "dash")
                    self.assertGreaterEqual(line["width"], 2.5)
                for name in snapshots[0]:
                    for before, after in zip(snapshots[0][name]["traces"], snapshots[1][name]["traces"]):
                        self.assertEqual(before["x"], after["x"])
                        self.assertEqual(before["y"], after["y"])

    def test_glance_window_starts_at_first_plotted_observation(self):
        helper = test_dispatch.DashboardTests()
        day = 24 * 60 * 60 * 1000
        for offsets in ((0, 60_000, 120_000), (0, day + 60_000, day + 120_000)):
            with self.subTest(offsets=offsets), tempfile.TemporaryDirectory() as directory:
                rows = helper.records(3)
                for row in rows[1:]:
                    row["queued_unix_ms"] = row["dispatch_unix_ms"] = helper.BASE + offsets[row["height"]]
                    row["observed_at_unix_ms"] = row["dispatch_unix_ms"] + 20
                glance = dashboard.collect(helper.write_records(directory, rows))["glance"]
                expected_first = 0 if offsets[-1] < day else 1
                self.assertEqual(glance["chart_start"], dashboard.utc(helper.BASE + offsets[expected_first]))
                self.assertEqual(glance["points"][0][0], expected_first)
                self.assertEqual(glance["window_start"], dashboard.utc(helper.BASE + offsets[-1] - day))

    def test_rolling_medians_limit_window_and_break_long_gaps(self):
        points = [[i, 123, i * 60_000, i + 1, 2 * (i + 1), 3 * (i + 1)] for i in range(17)]
        trend = dashboard.rolling_medians(points)
        self.assertEqual(trend[0], [0, 1, 2, 3])
        self.assertEqual(trend[1], [60_000, 1.5, 3, 4.5])
        self.assertEqual(trend[14][1:], [8, 16, 24])
        self.assertEqual(trend[15][1:], [9, 18, 27])
        points.append([17, 123, points[-1][2] + 600_000, 100, 200, 300])
        points.append([18, 123, points[-1][2] + 600_001, 200, 400, 600])
        trend = dashboard.rolling_medians(points)
        self.assertEqual(len(trend), len(points) + 1)
        self.assertEqual(trend[-3][1:], [11, 22, 33])
        self.assertEqual(trend[-2], [points[-1][2], None, None, None])
        self.assertEqual(trend[-1], [points[-1][2], 200, 400, 600])
        self.assertEqual(dashboard.rolling_medians([]), [])

    def test_glance_bin_ratios_and_savings_use_paired_full_period_values(self):
        helper = test_dispatch.DashboardTests()
        rows = helper.records(6)
        for row in rows[1:]:
            i = row["height"]
            row["block_bytes"] = (123, 123, 123, 20_000, 300_000, 300_001)[i]
            row["latency_ms"] = {"zakura": (2, 200, 202, 40, 60, 1),
                                 "zebra-reference": (99, 100, 102, 200, 300, 1),
                                 "zebra-candidate": (1, 100, 101, 10, 0, 1)}[row["target"]][i]
            if i == 0:
                row["queued_unix_ms"] = row["dispatch_unix_ms"] = helper.BASE - 2 * 86_400_000
                row["observed_at_unix_ms"] = row["dispatch_unix_ms"] + 20
            if i == 5:
                row["outcome"] = "rejected"
        with tempfile.TemporaryDirectory() as directory:
            data = dashboard.collect(helper.write_records(directory, rows))
        bins = data["glance"]["bins"]
        self.assertEqual(bins["<20 kB"]["zebra-reference"],
                         {"ratio": 102 / 101, "n": 3, "saving_ms": 1, "saving_n": 3})
        self.assertEqual(data["mixed_load"]["<20 kB"]["medians"]["zebra-reference"] -
                         data["mixed_load"]["<20 kB"]["medians"]["zebra-candidate"], 0)
        self.assertEqual(bins["20–300 kB"]["zebra-reference"],
                         {"ratio": 20, "n": 1, "saving_ms": 245, "saving_n": 2})
        self.assertEqual(bins["20–300 kB"]["zakura"]["ratio"], 4)
        self.assertEqual(bins[">300 kB"]["zebra-reference"],
                         {"ratio": None, "n": 0, "saving_ms": None, "saving_n": 0})
        self.assertEqual(data["glance"]["nodes"]["zebra-candidate"]["n"], 4)
        self.assertIn("<strong>20× faster</strong>", dashboard.render(data))

    def test_glance_short_build_label_uses_recorded_version(self):
        helper = test_dispatch.DashboardTests()
        version = "zebrad 6.2.2+1197.g89390f7 (source " + "a" * 40 + ", pasta-asm; sha256 " + "b" * 64 + ")"
        self.assertEqual(dashboard.short_build_label(version), "zebrad 6.2.2+1197.g89390f7, pasta-asm")
        self.assertEqual(dashboard.short_build_label("optimized-stack abc123"), "optimized-stack abc123")
        rows = helper.records()
        rows[0]["clients"]["zebra-candidate"]["version"] = version
        rows[-1]["version"] = version
        with tempfile.TemporaryDirectory() as directory:
            data = dashboard.collect(helper.write_records(directory, rows))
        html = dashboard.render(data)
        self.assertIn("<code>zebrad 6.2.2+1197.g89390f7, pasta-asm</code>", html)

    def test_comparison_labels_use_directional_five_percent_display_band(self):
        for ratio, expected in ((13.7, "13.7× faster"), (1.05, "1.05× faster"),
                                (1.049, "Within 5%"), (1.02, "Within 5%"), (1, "Within 5%"),
                                (.961, "Within 5%"), (1 / 1.05, "1.05× slower"),
                                (.90, "1.11× slower"), (None, "—"),
                                (0, "Slower (zero comparator latency)")):
            with self.subTest(ratio=ratio):
                self.assertEqual(dashboard.comparison_text(ratio), expected)
        helper = test_dispatch.DashboardTests()
        for ratio, headline, cell in (
                (13.7, "13.7× faster", "13.7× faster"),
                (1.02, "Within 5%", "within 5% (1.02×)"),
                (1, "Within 5%", "within 5% (1.00×)"),
                (.961, "Within 5%", "within 5% (0.961×)"),
                (.90, "1.11× slower", "1.11× slower")):
            with self.subTest(rendered_ratio=ratio), tempfile.TemporaryDirectory() as directory:
                rows = helper.records()
                for row in rows[1:]:
                    row["latency_ms"] = 100 if row["target"] == "zebra-candidate" else 100 * ratio
                html = dashboard.render(dashboard.collect(helper.write_records(directory, rows)))
                self.assertIn(f'<p class="glance-ratio">{headline}</p>', html)
                self.assertIn(f'<strong>{cell}</strong>', html)
                self.assertNotIn("1× faster", html)
                if ratio == 1:
                    self.assertIn("Median difference &lt; 0.05 ms per block", html)

    def test_saving_labels_preserve_sign_and_use_magnitude_precision(self):
        for value, expected in (
                (None, "Median difference unavailable"),
                (0, "Median difference < 0.05 ms per block"),
                (.049, "Median difference < 0.05 ms per block"),
                (-.049, "Median difference < 0.05 ms per block"),
                (.05, "Saves 0.05 ms per block"), (-.05, "Adds 0.05 ms per block"),
                (.12, "Saves 0.12 ms per block"), (-.12, "Adds 0.12 ms per block"),
                (1.4, "Saves 1.4 ms per block"), (-1.4, "Adds 1.4 ms per block"),
                (113, "Saves 113 ms per block"), (-113, "Adds 113 ms per block")):
            with self.subTest(value=value):
                self.assertEqual(dashboard.saving_text(value), expected)


if __name__ == "__main__":
    unittest.main()
