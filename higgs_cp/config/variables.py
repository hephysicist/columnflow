
### This config is used for listing the variables used in the analysis ###

from columnflow.config_util import add_category

import order as od

from columnflow.columnar_util import EMPTY_FLOAT


def add_variables(cfg: od.Config) -> None:
    # add variables
    # (the "event", "run" and "lumi" variables are required for some cutflow plotting task,
    # and also correspond to the minimal set of columns that coffea's nano scheme requires)
    cfg.add_variable(
        name="event",
        expression="event",
        binning=(1, 0.0, 1.0e9),
        x_title="Event number",
        discrete_x=True,
    )
    cfg.add_variable(
        name="run",
        expression="run",
        binning=(1, 100000.0, 500000.0),
        x_title="Run number",
        discrete_x=True,
    )
    cfg.add_variable(
        name="lumi",
        expression="luminosityBlock",
        binning=(1, 0.0, 5000.0),
        x_title="Luminosity block",
        discrete_x=True,
    )
    cfg.add_variable(
        name="n_jet",
        expression="n_jet",
        binning=(11, -0.5, 10.5),
        x_title="Number of jets",
        discrete_x=True,
    )
    cfg.add_variable(
        name="jets_pt",
        expression="Jet.pt",
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"$p_{T} of all jets$",
    )
    cfg.add_variable(
        name="jet1_pt",
        expression="Jet.pt[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"Jet 1 $p_{T}$",
    )
    cfg.add_variable(
        name="jet1_eta",
        expression="Jet.eta[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(30, -3.0, 3.0),
        x_title=r"Jet 1 $\eta$",
    )
    # cfg.add_variable(
    #     name="ht",
    #     expression=lambda events: ak.sum(events.Jet.pt, axis=1),
    #     binning=(40, 0.0, 800.0),
    #     unit="GeV",
    #     x_title="HT",
    # )
    # weights
    cfg.add_variable(
        name="mc_weight",
        expression="mc_weight",
        binning=(200, -10, 10),
        x_title="MC weight",
    )
    # cutflow variables
    cfg.add_variable(
        name="cf_jet1_pt",
        expression="cutflow.jet1_pt",
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"Jet 1 $p_{T}$",
    )
    
    cfg.add_variable(
        name="muon_eta",
        expression="Muon.eta",
        null_value=EMPTY_FLOAT,
        binning=(30, -3.0, 3.0),
        x_title=r"Raw muon $\eta$",
    )
    
    cfg.add_variable(
        name="muon_pt",
        expression="Muon.pt",
        null_value=EMPTY_FLOAT,
        binning=(40, 0.0, 100.0),
        unit="GeV",
        x_title=r"Raw muon $p_{T}$",
    )
    cfg.add_variable(
        name="muon_phi",
        expression="Muon.phi",
        null_value=EMPTY_FLOAT,
        binning=(40, 0, 6.28),
        x_title=r"Raw  muon $phi$",
    )