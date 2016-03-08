package org.fao.fenix.d3p.flow.impl;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.process.dto.StepId;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.TableStep;
import org.fao.fenix.d3p.flow.Flow;
import org.fao.fenix.d3p.flow.FlowProperties;
import org.fao.fenix.d3p.process.Process;
import org.fao.fenix.d3p.process.ProcessFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@FlowProperties(name = "chain", global = true, priority = 1)
public class Chain extends Flow {
    private @Inject StepFactory stepFactory;
    private @Inject ProcessFactory factory;
    private @Inject CacheFactory cacheFactory;
    private @Inject UIDUtils uidUtils;


    @Override
    public Map<StepId, Resource<DSDDataset,Object[]>> process(Map<StepId,TableStep> sourceSteps, Set<StepId> resultRidList, Process[] processes, org.fao.fenix.commons.process.dto.Process[] flow) throws Exception {
        //Verify applicability
        if (isSingleChain(sourceSteps, resultRidList, flow))
            throw new UnsupportedOperationException();

        //Retrieve source information
        Step previousStep = sourceSteps.values().iterator().next();
        Map<StepId, Object[]> flowBySid = getFlowBySid(flow, processes);

        //Apply flow
        for (Object[] nextProcess = flowBySid.get(sourceSteps.keySet().iterator()); nextProcess!=null; nextProcess = flowBySid.get(((org.fao.fenix.commons.process.dto.Process)nextProcess[0]).getRid())) {
            //Run next process and retrieve result
            Step currentStep = ((Process) nextProcess[1]).process(((org.fao.fenix.commons.process.dto.Process) nextProcess[0]).getParameters(), new Step[]{previousStep});
            //Result completion and dsd normalization
            if (currentStep.getRid()==null)
                currentStep.setRid(((org.fao.fenix.commons.process.dto.Process) nextProcess[0]).getRid());
            if (currentStep.getStorage()==null)
                currentStep.setStorage(previousStep.getStorage());
            if (currentStep.getDsd()==null)
                currentStep.setDsd(previousStep.getDsd());
            currentStep.getDsd().setContextSystem("D3P");
            currentStep.getDsd().setDatasources(null);
            currentStep.getDsd().setRID(null);

            previousStep = currentStep;
        }

        //Generate and return in-memory resource from the last step
        Map<StepId, Resource<DSDDataset,Object[]>> result = new HashMap<>();
        result.put(previousStep.getRid(),previousStep.getResource());
        return result;
    }


    //Utils
    private boolean isSingleChain(Map<StepId,TableStep> sourceSteps, Set<StepId> resultRidList, org.fao.fenix.commons.process.dto.Process[] flow) {
        if (sourceSteps.size()==1 && resultRidList.size()==1) {
            for (org.fao.fenix.commons.process.dto.Process processInfo : flow)
                if (processInfo.getSid().length!=1)
                    return false;
            return true;
        }
        return false;
    }

    private Map<StepId, Object[]> getFlowBySid(org.fao.fenix.commons.process.dto.Process[] flow, Process[] processes) {
        Map<StepId, Object[]> flowBySid = new HashMap<>();
        for (int i=0; i<flow.length; i++)
            flowBySid.put(flow[i].getSid()[0], new Object[]{flow[i],processes[i]});
        return flowBySid;
    }
}
