package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.templates.identification.MeIdentification;
import org.fao.fenix.commons.msd.dto.templates.standard.combined.dataset.DSD;
import org.fao.fenix.d3p.dto.type.DomainCompletionStatus;

import java.util.Collection;

public class DomainStatus {

    private DSD resource;
    private DomainCompletionStatus status;
    private Integer count;
    private Collection<MeIdentification> resources;


    public DSD getResource() {
        return resource;
    }

    public void setResource(DSD resource) {
        this.resource = resource;
    }

    public DomainCompletionStatus getStatus() {
        return status;
    }

    public void setStatus(DomainCompletionStatus status) {
        this.status = status;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Collection<MeIdentification> getResources() {
        return resources;
    }

    public void setResources(Collection<MeIdentification> resources) {
        this.resources = resources;
    }
}
