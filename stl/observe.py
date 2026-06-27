def render_observe(metrics: dict) -> str:
    rows = []
    for show_id in sorted(metrics):
        m = metrics[show_id]
        alert = "alert" if m["dropped"] else "ok"
        rows.append(
            f"<tr class='{alert}'><td>#{show_id}</td><td>{m['events']}</td>"
            f"<td>{m['late_allowed']}</td><td>{m['dropped']}</td>"
            f"<td>{m['watermark_lag']:.1f}s</td></tr>"
        )
    table = "\n".join(rows)
    return f"""<!doctype html><html><head><meta charset=utf-8>
<title>stream-truth-layer — per-tenant SLOs</title>
<style>
body{{background:#0a0a0a;color:#e5e5e5;font-family:ui-monospace,Menlo,monospace;padding:2rem}}
h1{{color:#22c55e;font-weight:600}} table{{border-collapse:collapse;width:100%;max-width:720px}}
th,td{{border:1px solid #262626;padding:.5rem .75rem;text-align:left}}
th{{color:#a3a3a3;font-weight:500}} tr.alert td{{color:#ef4444}}
caption{{text-align:left;color:#737373;margin-bottom:1rem}}
</style></head><body>
<h1>Per-tenant SLOs</h1>
<table><caption>watermark lag = liveness · dropped = correctness risk (late beyond allowed lateness)</caption>
<tr><th>show</th><th>events</th><th>late (allowed)</th><th>dropped</th><th>watermark lag</th></tr>
{table}
</table></body></html>"""
