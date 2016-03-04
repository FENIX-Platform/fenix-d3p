package org.fao.fenix.d3p.flow.impl;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.process.dto.StepId;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3p.dto.NoDataException;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.TableStep;
import org.fao.fenix.d3p.flow.Flow;
import org.fao.fenix.d3p.flow.FlowProperties;
import org.fao.fenix.d3p.flow.impl.graph.dto.Node;
import org.fao.fenix.d3p.process.Process;
import org.fao.fenix.d3p.process.ProcessFactory;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.sql.Connection;
import java.util.*;

@FlowProperties(name = "graph", global = true, priority = 2)
public class Graph extends Flow {
    private @Inject StepFactory stepFactory;
    private @Inject ProcessFactory factory;
    private @Inject CacheFactory cacheFactory;
    private @Inject UIDUtils uidUtils;

    private Collection<Node> getGraph (Map<StepId, Node> nodesById, Map<StepId,TableStep> sourceSteps, org.fao.fenix.commons.process.dto.Process[] flow) {
        //Create nodes
        for (org.fao.fenix.commons.process.dto.Process processInfo : flow)
            nodesById.put(processInfo.getRid(), new Node(processInfo));
        for (StepId sid : sourceSteps.keySet())
            nodesById.put(sid, new Node(sid));

        //Link nodes
        for (org.fao.fenix.commons.process.dto.Process processInfo : flow) {
            Node node = nodesById.get(processInfo.getRid());
            for (StepId sid : processInfo.getSid())
                node.addPrev(nodesById.get(sid));
        }

        //Return source nodes
        Collection<Node> nodes = new LinkedList<>();
        for (Node node : nodesById.values())
            if (node.isSource())
                nodes.add(node);
        return nodes;
    }


    @Override
    public Resource<DSDDataset,Object[]> process(Map<StepId,TableStep> sourceSteps, Set<StepId> resultRidList, Process[] processes, org.fao.fenix.commons.process.dto.Process[] flow) throws Exception {
        //Build graph
        Map<StepId, Node> nodesById = new HashMap<>();
        Collection<Node> sourceNodes = getGraph(nodesById, sourceSteps, flow);

        //Retrieve source information
        Step currentStep = sourceSteps.values().iterator().next();
        Map<StepId, Object[]> flowBySid = getFlowBySid(flow, processes);

        try {
            //Apply flow
            for (Object[] nextProcess = flowBySid.get(sourceSteps.keySet().iterator()); nextProcess != null; nextProcess = flowBySid.get(((org.fao.fenix.commons.process.dto.Process) nextProcess[0]).getRid()))
                currentStep = ((Process) nextProcess[1]).process(((org.fao.fenix.commons.process.dto.Process) nextProcess[0]).getParameters(), new Step[]{currentStep});

            //Generate and return in-memory resource from the last step
            return currentStep.getResource();
        } catch(NoDataException ex) {
            throw new BadRequestException("Multiple iterator step consuming identified into the flow");
        }
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
