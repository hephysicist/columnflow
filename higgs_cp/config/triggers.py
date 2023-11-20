# coding: utf-8

"""
Definition of triggers
"""

import order as od

from higgs_cp.config.util import Trigger, TriggerLeg


def add_triggers_run3_2022_postEE(config: od.Config) -> None:
    """
    Adds all triggers to a *config*. For the conversion from filter names to trigger bits, see
    https://github.com/cms-sw/cmssw/blob/master/PhysicsTools/NanoAOD/python/triggerObjects_cff.py.
    """
    config.x.triggers = od.UniqueObjectIndex(Trigger,[
        
        #
        # single muon
        #
        Trigger(
            name="HLT_IsoMu24",
            id=131, #13 is for muon pdg_id, 1 because it's first muon trigger
            legs=[
                TriggerLeg(
                    pdg_id=13,
                    min_pt=25.0,
                    trigger_bits=2,
                ),
            ],
            tags={"single_trigger", "single_mu", "channel_mu_tau"},
        ),
        Trigger(
            name="HLT_IsoMu27",
            id=132, #13 is for muon pdg_id, 1 because it's first muon trigger
            legs=[
                TriggerLeg(
                    pdg_id=13,
                    min_pt=25.0,
                    trigger_bits=2,
                ),
            ],
            tags={"single_trigger", "single_mu", "channel_mu_tau"},
        ),
        #
        # cross-trigger
        #
        Trigger(
            name="HLT_IsoMu20_eta2p1_LooseDeepTauPFTauHPS27_eta2p1_CrossL1",
            id=301,
            legs=[
                TriggerLeg(
                    pdg_id=13, #muon
                    min_pt=21.0,
                    trigger_bits=2 + 64, #TODO: Understand these numbers 
                ),
                TriggerLeg(
                    pdg_id=15, #tau
                    min_pt=32.0,
                    trigger_bits=1024 + 512, #TODO: Understand these numbers 
                ),
            ],
            tags={"cross_trigger", "cross_mu_tau", "channel_mu_tau"},
        ),
    ])