# coding: utf-8

"""
Configuration of the higgs_cp analysis.
"""

import law
import order as od



#
# the main analysis object
#

analysis_higgs_cp = analysis_hcp = od.Analysis(name="analysis_higgs_cp", id=1,)

#analysis_hcplysis-global versions
# (see cfg.x.versions below for more info)
analysis_hcp.x.versions = {}

# files of bash sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
analysis_hcp.x.bash_sandboxes = ["$CF_BASE/sandboxes/cf.sh"]
default_sandbox = law.Sandbox.new(law.config.get("analysis", "default_columnar_sandbox"))
if default_sandbox.sandbox_type == "bash" and default_sandbox.name not in analysis_hcp.x.bash_sandboxes:
   analysis_hcp.x.bash_sandboxes.append(default_sandbox.name)

# files of cmssw sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
analysis_hcp.x.cmssw_sandboxes = [
    "$CF_BASE/sandboxes/cmssw_default.sh",
]

# clear the list when cmssw bundling is disabled
#if not law.util.flag_to_bool(os.getenv("H4L_BUNDLE_CMSSW", "1")):
#    del ana.x.cmssw_sandboxes[:]

# config groups for conveniently looping over certain configs
# (used in wrapper_factory)
analysis_hcp.x.config_groups = {}

from higgs_cp.config.run_3_campaign import add_run3_campaign
from cmsdb.campaigns.run3_2022_postEE_nano_tau_v12 import campaign_run3_2022_postEE_nano_tau_v12

add_run3_campaign(analysis_hcp,
                  campaign_run3_2022_postEE_nano_tau_v12.copy(),
                  config_name=f"{campaign_run3_2022_postEE_nano_tau_v12.name}_limited",
                  config_id = 22,
                  limit_dataset_files = 1)


from higgs_cp.config.run_2_ul2018_campaign import add_run_2_ul2018_campaign
from cmsdb.campaigns.run2_UL2018_nano_tau_v10 import campaign_run2_UL2018_nano_tau_v10 


add_run_2_ul2018_campaign(analysis_hcp,
                          campaign_run2_UL2018_nano_tau_v10.copy(),
                          config_name=f"{campaign_run2_UL2018_nano_tau_v10.name}_limited",
                          config_id = 15,
                          limit_dataset_files = 1)




