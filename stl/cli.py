from pathlib import Path
import typer
from stl.pipelines.reconcile import reconcile as run_reconcile
from stl.pipelines.recovery import run_with_crash
from stl.pipelines.batch import run_batch
from stl.pipelines.naive import run_naive
from stl.pipelines.event_time import run_event_time
from stl.seeds import load_events
from stl.explain import explain as run_explain
from stl.observe import render_observe

app = typer.Typer(help="stream-truth-layer: a mini-Flink truth gate for live auctions")


def _dollars(c: int) -> str:
    return f"${c/100:.2f}"


@app.command()
def run(mode: str = "event-time", inject_late: bool = True):
    events = load_events(inject_late=inject_late)
    if mode == "naive":
        table = run_naive(events)
    elif mode == "batch":
        table = run_batch(events)
    else:
        table, _m, _s = run_event_time(events)
    for k in sorted(table):
        row = table[k]
        typer.echo(f"{k}: winner={row['winner']} @ {_dollars(row['winning_bid_cents'])}")


@app.command()
def reconcile(inject_late: bool = True):
    r = run_reconcile(inject_late=inject_late)
    typer.echo(f"event-time matches oracle: {r['matches_oracle']}")
    for d in r["naive_divergences"]:
        typer.echo(
            f"  DIVERGENCE {d['key']}: naive={d['naive_winner']} "
            f"{_dollars(d['naive_bid_cents'])}  oracle={d['oracle_winner']} "
            f"{_dollars(d['oracle_bid_cents'])}"
        )
    for d in r["late_drops"]:
        typer.echo(
            f"  SLO DROP show #{d['show_id']}: {d['dropped']} bid(s) beyond "
            f"allowed lateness (excluded from parity, surfaced in observe)"
        )
    if not r["matches_oracle"]:
        raise typer.Exit(1)


@app.command("checkpoint-restore")
def checkpoint_restore(crash_at: int = 3):
    r = run_with_crash(load_events(inject_late=True), crash_at=crash_at)
    typer.echo(f"clean GMV:     {r['clean_gmv']}")
    typer.echo(f"recovered GMV: {r['recovered_gmv']}")
    typer.echo(f"at-least-once would double-count to: {r['double_count_gmv']}")
    typer.echo(f"exactly-once preserved: {r['exactly_once']}")
    if not r["exactly_once"]:
        raise typer.Exit(1)


@app.command()
def observe(out: Path = Path("demo/observe.html")):
    r = run_reconcile(inject_late=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_observe(r["metrics"]))
    typer.echo(f"wrote {out}")


@app.command()
def explain(use_ollama: bool = False):
    typer.echo(run_explain(use_ollama=use_ollama))


@app.command()
def version():
    typer.echo("stream-truth-layer 0.1.0")
