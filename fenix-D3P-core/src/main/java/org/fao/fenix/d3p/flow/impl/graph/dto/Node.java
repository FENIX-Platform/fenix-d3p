package org.fao.fenix.d3p.flow.impl.graph.dto;

import org.fao.fenix.commons.process.dto.StepId;
import org.fao.fenix.d3p.dto.NodeSources;
import org.fao.fenix.d3p.dto.Step;

import java.util.Collection;
import java.util.LinkedList;

public class Node {

    public StepId id;
    public Collection<Node> prev = new LinkedList<>();
    public Collection<Node> next = new LinkedList<>();
    public Boolean result;
    public int index = -1;

    public NodeSources sources = new NodeSources();


    public Node() {
    }
    public Node(StepId stepId) {
        id = stepId;
    }
    public Node(org.fao.fenix.commons.process.dto.Process processInfo) {
        index = processInfo.index;
        result = processInfo.getResult();
        id = processInfo.getRid();
        sources.init(processInfo.getSid());
    }

    //Utils
    public boolean isOneToMany() {
        return next.size()+((result!=null && result)?1:0)>1;
    }
    public boolean isManyToOne() {
        return prev.size()>1;
    }
    public boolean isSource() {
        return prev.size()==0;
    }
    public boolean isResult() {
        return result==null ? next.size()==0 : result;
    }

    public void addPrev(Node node) {
        prev.add(node);
        node.next.add(this);
    }
}
