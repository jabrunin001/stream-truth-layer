from dataclasses import dataclass

SIZE = 10.0
ALLOWED_LATENESS = 5.0
MAX_LATENESS = 5.0


@dataclass
class WindowResult:
    show_id: int
    window_start: float
    window_end: float
    winner: str | None
    winning_bid_cents: int
    late_firing: bool


def window_key(show_id: int, window_start: float) -> str:
    return f"{show_id}|{window_start:g}"


def to_table(results) -> dict:
    return {
        window_key(r.show_id, r.window_start): {
            "show_id": r.show_id, "window_start": r.window_start,
            "winner": r.winner, "winning_bid_cents": r.winning_bid_cents,
        }
        for r in results
    }
