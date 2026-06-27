import subprocess
from stl.pipelines.reconcile import reconcile


def _dollars(cents: int) -> str:
    return f"{cents / 100:.2f}"


def _deterministic() -> str:
    r = reconcile(inject_late=True)
    lines = [
        "stream-truth-layer — what the watermark gate caught:",
        "",
        "Show #42 (Charizard final, window [0,10)):",
    ]
    for d in r["naive_divergences"]:
        if d["key"].startswith("42|"):
            lines.append(
                f"  Naive processing-time windowing crowned {d['naive_winner']} "
                f"at ${_dollars(d['naive_bid_cents'])} — it bucketed "
                f"{d['oracle_winner']}'s late ${_dollars(d['oracle_bid_cents'])} "
                f"bid into the next window."
            )
            lines.append(
                f"  Event-time windowing with a 5s watermark matched the batch "
                f"oracle: {d['oracle_winner']} wins at "
                f"${_dollars(d['oracle_bid_cents'])}."
            )
    lines.append("")
    lines.append(f"  Streaming == batch oracle: {r['matches_oracle']}.")
    return "\n".join(lines)


def explain(use_ollama: bool = False) -> str:
    base = _deterministic()
    if not use_ollama:
        return base
    try:
        out = subprocess.run(
            ["ollama", "run", "llama3.2",
             "Rewrite this incident note for an engineer, keep all names and "
             "dollar amounts exactly:\n\n" + base],
            capture_output=True, text=True, timeout=20, check=True,
        )
        return out.stdout.strip() or base
    except Exception:
        return base
