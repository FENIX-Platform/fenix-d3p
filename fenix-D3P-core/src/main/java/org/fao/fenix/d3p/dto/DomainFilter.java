package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.find.dto.filter.DataFilter;
import org.fao.fenix.commons.find.dto.filter.StandardFilter;

import java.util.Collection;

public class DomainFilter {

    private StandardFilter resources;
    private DataFilter data;
    private Collection<Process> flow;

    public StandardFilter getResources() {
        return resources;
    }

    public void setResources(StandardFilter resources) {
        this.resources = resources;
    }

    public DataFilter getData() {
        return data;
    }

    public void setData(DataFilter data) {
        this.data = data;
    }

    public Collection<Process> getFlow() {
        return flow;
    }

    public void setFlow(Collection<Process> flow) {
        this.flow = flow;
    }
}
