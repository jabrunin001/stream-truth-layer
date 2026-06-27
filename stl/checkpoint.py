import copy
from dataclasses import dataclass


@dataclass
class Checkpoint:
    state: dict
    offset: int
    watermarks: dict


def take(state, source, watermarks) -> Checkpoint:
    return Checkpoint(
        state=copy.deepcopy(state.snapshot()),
        offset=source.offset,
        watermarks={sid: wm.snapshot() for sid, wm in watermarks.items()},
    )


def restore_into(ckpt: Checkpoint, state, source, watermarks_factory) -> dict:
    state.restore(ckpt.state)
    source.seek(ckpt.offset)
    watermarks = {}
    for sid, snap in ckpt.watermarks.items():
        wm = watermarks_factory()
        wm.restore(snap)
        watermarks[sid] = wm
    return watermarks
