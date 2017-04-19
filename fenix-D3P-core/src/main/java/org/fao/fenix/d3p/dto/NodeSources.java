package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.process.dto.StepId;

import java.sql.Connection;
import java.util.*;

public class NodeSources extends LinkedList<Step> {
    private final Map<StepId, Integer> sourcesIndex = new HashMap<>();

    private Comparator<Step> sourceComparator = new Comparator<Step>() {
        @Override
        public int compare(Step o1, Step o2) {
            Integer index1 = o1!=null ? sourcesIndex.get(o1.getRid()) : null;
            Integer index2 = o2!=null ? sourcesIndex.get(o2.getRid()) : null;
            return (index1!=null ? index1 : new Integer(-1)).compareTo(index2!=null ? index2 : new Integer(-1));
        }
    };

    public NodeSources() { }
    public NodeSources(StepId[] sources) {
        init(sources);
    }
    public void init(StepId[] sources) {
        if (sources!=null)
            for (int i=0; i<sources.length; i++)
                sourcesIndex.put(sources[i],i);
    }

    @Override
    public boolean add(Step step) {
        boolean added = super.add(step);
        Collections.sort(this,sourceComparator);
        return added;
    }

}
