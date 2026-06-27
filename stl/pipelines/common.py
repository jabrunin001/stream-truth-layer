from dataclasses import dataclass


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
