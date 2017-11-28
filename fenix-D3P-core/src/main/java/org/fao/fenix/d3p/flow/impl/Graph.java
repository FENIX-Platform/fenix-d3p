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

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

@FlowProperties(name = "graph", global = true, priority = 2)
public class Graph extends Flow {
    private @Inject StepFactory stepFactory;
    private @Inject ProcessFactory factory;
    private @Inject CacheFactory cacheFactory;
    private @Inject UIDUtils uidUtils;


    @Override
    public Map<StepId, Resource<DSDDataset,Object[]>> process(Map<StepId,TableStep> sourceSteps, Set<StepId> resultRidList, Process[] processes, org.fao.fenix.commons.process.dto.Process[] flow, boolean lazy) throws Exception {
        //Build graph
        Map<StepId, Node> nodesById = new HashMap<>();
        Collection<Node> sourceNodes = getGraph(nodesById, sourceSteps, flow);

        try {
            //Apply flow
            Map<StepId, Resource<DSDDataset,Object[]>> result = new HashMap<>();
            for (Step source : sourceSteps.values())
                traverse(nodesById, processes, flow, result, source, lazy);
            return result;
        } catch(NoDataException ex) {
            throw new BadRequestException("Multiple iterator step consuming identified into the flow");
        }
    }

    private void traverse (Map<StepId, Node> nodesById, Process[] processes, org.fao.fenix.commons.process.dto.Process[] flow, Map<StepId, Resource<DSDDataset,Object[]>> result, Step source, boolean lazy) throws Exception {
        //Retrieve previous process info
        Node previousNode = nodesById.get(source.getRid());
        source.setOneToMany(previousNode.isOneToMany());
        //Store result
        if (previousNode.isResult())
            result.put(source.getRid(), source.getResource(lazy));
        //Apply next process steps
        for (Node nextNode : previousNode.next) {
            nextNode.sources.add(source);
            //Verify source steps availability and apply process
            if (nextNode.prev.size()==nextNode.sources.size()) {
                //Run next process and retrieve result
                Step nextNodeResult = processes[nextNode.index].process(flow[nextNode.index].getParameters(), nextNode.sources.toArray(new Step[nextNode.sources.size()]));
                //Result completion and dsd normalization
                if (nextNodeResult.getRid()==null)
                    nextNodeResult.setRid(flow[nextNode.index].getRid());
                if (nextNodeResult.getStorage()==null)
                    nextNodeResult.setStorage(source.getStorage());
                DSDDataset dsd = nextNodeResult.getCurrentDsd();
                if (dsd==null)
                    nextNodeResult.setDsd(dsd = source.getDsd());
                dsd.setContextSystem("D3P");
                dsd.setDatasources(null);
                dsd.setRID(null);
                //Propagation into the graph
                traverse(nodesById, processes, flow, result, nextNodeResult, lazy);
            }
        }
    }

    //Utils
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
}
