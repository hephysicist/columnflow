# coding: utf-8

"""
Tasks to plot different types of histograms.
"""

from collections import OrderedDict
from abc import abstractmethod

import law
import luigi

from columnflow.tasks.framework.base import Requirements, ShiftTask
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin,
    CategoriesMixin, ShiftSourcesMixin,
)
from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase1D, PlotBase2D, ProcessPlotSettingMixin, VariablePlotSettingMixin,
)
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import DotDict, dev_sandbox, dict_add_strict


class DataDrivenEstimationBase(
    VariablePlotSettingMixin,
    ProcessPlotSettingMixin,
    CategoriesMixin,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))
    """sandbox to use for this task. Defaults to *default_columnar_sandbox* from
    analysis config.
    """

    exclude_index = True

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeHistograms=MergeHistograms,
    )
    """Set upstream requirements, in this case :py:class:`~columnflow.tasks.histograms.MergeHistograms`
    """

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "plot", f"datasets_{self.datasets_repr}")
        return parts

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name})
            for cat_name in sorted(self.categories)
            for var_name in sorted(self.variables)
        ]

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["merged_hists"] = self.requires_from_branch()

        return reqs

    @abstractmethod
    def get_plot_shifts(self):
        return
    
    @law.decorator.log
    @view_output_plots
    def run(self):
        import hist
        import numpy as np
        from cmsdb.processes.qcd import qcd

        # get the shifts to extract and plot
        plot_shifts = law.util.make_list(self.get_plot_shifts())

        # prepare config objects
        variable_tuple = self.variable_tuples[self.branch_data.variable]
        variable_insts = [
            self.config_inst.get_variable(var_name)
            for var_name in variable_tuple
        ]
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = {}
        if category_inst.name == 'cat_c':
            with self.publish_step(f"estimating qcd for {self.branch_data.variable} in {category_inst.name}"):
                for dataset, inp in self.input().items():
                    dataset_inst = self.config_inst.get_dataset(dataset)
                    h_in = inp["collection"][0]["hists"].targets[self.branch_data.variable].load(formatter="pickle")

                    # loop and extract one histogram per process
                    for process_inst in process_insts:
                        # skip when the dataset is already known to not contain any sub process
                        if not any(map(dataset_inst.has_process, sub_process_insts[process_inst])):
                            continue

                        # work on a copy
                        h = h_in.copy()

                        # axis selections
                        h = h[{
                            "process": [
                                hist.loc(p.id)
                                for p in sub_process_insts[process_inst]
                                if p.id in h.axes["process"]
                            ],
                            "category": [
                                hist.loc(c.id)
                                for c in leaf_category_insts
                                if c.id in h.axes["category"]
                            ],
                            "shift": [
                                hist.loc(s.id)
                                for s in plot_shifts
                                if s.id in h.axes["shift"]
                            ],
                        }]

                        # axis reductions
                        h = h[{"process": sum, "category": sum}]

                        # add the histogram
                        if process_inst in hists:
                            hists[process_inst] += h
                        else:
                            hists[process_inst] = h

                # there should be hists to plot
                if not hists:
                    raise Exception(
                        "no histograms found to plot; possible reasons:\n" +
                        "  - requested variable requires columns that were missing during histogramming\n" +
                        "  - selected --processes did not match any value on the process axis of the input histogram",
                    )

                # sort hists by process order
                hists = OrderedDict(
                    (process_inst.copy_shallow(), hists[process_inst])
                    for process_inst in sorted(hists, key=process_insts.index)
                )
                
                qcd_hist = None
                qcd_hist_values = None
                for process_inst, h in hists.items():
                    hist_np , _ , _ = h.to_numpy(flow=True)
                    if qcd_hist is None:
                        qcd_hist = h.copy()
                        qcd_hist_values = np.zeros_like(hist_np)
                    if process_inst.is_data: qcd_hist_values += hist_np
                    else: qcd_hist_values -= hist_np
                
                #if the array contains negative values, set them to zero
                qcd_hist_values = np.where(qcd_hist_values > 0, qcd_hist_values, 0)
                qcd_hist.view(flow=True).value[:] = qcd_hist_values
                qcd_hist.view(flow=True).variance[:] = np.zeros_like(qcd_hist_values)
                qcd_hist
                #register a new datased at the hlist
                hists[qcd] = qcd_hist
                #save qcd estimation histogram and plots only for control region
                
                self.output()["qcd_hists"][self.branch_data.variable].dump(qcd_hist, formatter="pickle")
                # call the plot function
                fig, _ = self.call_plot_func(
                    self.plot_function,
                    hists=hists,
                    config_inst=self.config_inst,
                    category_inst=category_inst.copy_shallow(),
                    variable_insts=[var_inst.copy_shallow() for var_inst in variable_insts],
                    **self.get_plot_parameters(),
                )

                # save the plot
                for outp in self.output()["plots"]:
                    outp.dump(fig, formatter="mpl")


class DataDrivenEstimationSingleShift(
    DataDrivenEstimationBase,
    ShiftTask,
):
    exclude_index = True

    # upstream requirements
    reqs = Requirements(
        DataDrivenEstimationBase.reqs,
        MergeHistograms=MergeHistograms,
    )

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name})
            for var_name in sorted(self.variables)
            for cat_name in sorted(self.categories)
        ]

    def requires(self):
        return {
            d: self.reqs.MergeHistograms.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def output(self):
        b = self.branch_data
        return {"plots": [
            self.target(name)
            for name in self.get_plot_names(f"plot__proc_{self.processes_repr}__cat_{b.category}__var_{b.variable}")
        ],
        "qcd_hists": law.SiblingFileCollection({
            variable_name: self.target(f"qcd_histogram__{b.category}_{variable_name}.pickle")
            for variable_name in self.variables
        })}

    def get_plot_shifts(self):
        return [self.global_shift_inst]


class DataDrivenEstimation(
    DataDrivenEstimationSingleShift,
    DataDrivenEstimationBase,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_per_process",
        add_default_to_description=True,
    )