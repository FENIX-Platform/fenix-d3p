package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.find.dto.filter.StandardFilter;

import java.util.Collection;

public class DomainFilter {

    private StandardFilter resources;
    private DataFilter data;

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
}
