from operator import and_
from functools import reduce
from collections import defaultdict

from columnflow.production.util import attach_coffea_behavior
from columnflow.production.categories import category_ids
from columnflow.production.processes import process_ids
# from columnflow.production.cms.mc_weight import mc_weight
# from columnflow.production.cms.pileup import pu_weight
# from columnflow.production.cms.scale import murmuf_weights
# from columnflow.selection.cms.json_filter import json_filter
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.stats import increment_stats
from higgs_cp.selection.trigger import trigger_selection



from columnflow.util import maybe_import, dev_sandbox
from higgs_cp.production.example import cutflow_features


np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    uses={
        "event",
        attach_coffea_behavior, 
        trigger_selection,
        cutflow_features,
        process_ids,
        category_ids,
        increment_stats,
        
        # json_filter, mc_weight, electron_selection,
        # trigger_selection,
        # category_ids,
        # pu_weight, murmuf_weights, increment_stats, process_ids, muon_selection,
    },
    produces={
        attach_coffea_behavior, 
        trigger_selection,
        cutflow_features,
        process_ids,
        category_ids,
        increment_stats,
        # json_filter, mc_weight, electron_selection,
        # category_ids,
        # pu_weight, murmuf_weights, increment_stats, process_ids, muon_selection,
    },
    sandbox=dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar_dev.sh"),
    exposed=True,
)
def default(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    
    # ensure coffea behaviors are loaded
    events = self[attach_coffea_behavior](events, **kwargs)
    results = SelectionResult()
    events, trigger_results = self[trigger_selection](events, call_force=True, **kwargs)
    
    results += trigger_results
    
   # write out process IDs
    events = self[process_ids](events, **kwargs)
    events = self[category_ids](events, results=results, **kwargs)
    
    event_sel = reduce(and_, results.steps.values())
    results.main["event"] = event_sel

    # keep track of event counts, sum of weights
    # in several categories
    weight_map = {
        "num_events": Ellipsis,
        "num_events_selected": event_sel,
    }
    group_map = {}
    group_combinations = []
    
    group_map = {
        **group_map,
            # per process
            "process": {
                "values": events.process_id,
                "mask_fn": (lambda v: events.process_id == v),
            },
    }
    group_combinations.append(("process",))
            
    events, results = self[increment_stats](
        events,
        results,
        stats,
        weight_map=weight_map,
        group_map=group_map,
        group_combinations=group_combinations,
        **kwargs,
    )
    
    return events, results
