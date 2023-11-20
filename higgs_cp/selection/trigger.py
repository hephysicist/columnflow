
from __future__ import annotations

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector
def trigger_selection(
    self: Selector,
    events: ak.Array,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:

    # start with an all-false mask
    sel_trigger = ak.Array(np.zeros(len(events), dtype=bool))

    # pick events that passed one of the required triggers
    for trigger in self.dataset_inst.x("require_triggers"):
        print(f"Requiring trigger: {trigger}")
        single_fired = events.HLT[trigger]
        sel_trigger = sel_trigger | single_fired

    return events, SelectionResult(
        steps={
            "trigger": sel_trigger,
        },
    )


@trigger_selection.init
def trigger_selection_init(self: Selector) -> None:
    # return immediately if dataset object has not been loaded yet
    if not getattr(self, "dataset_inst", None):
        return

    # add HLT trigger bits to uses
    self.uses |= {
        f"HLT.{trigger}"
        for trigger in self.dataset_inst.x.require_triggers
    }
