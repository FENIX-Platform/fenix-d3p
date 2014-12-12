package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.templates.identification.MeIdentification;
import org.fao.fenix.commons.msd.dto.templates.standardDsd.dataset.MeIdentificationDSDOnly;
import org.fao.fenix.d3p.dto.type.DomainCompletionStatus;

import java.util.Collection;

public class DomainStatus {

    private MeIdentificationDSDOnly resource;
    private DomainCompletionStatus status;
    private Integer count;
    private Collection<MeIdentification> resources;


    public MeIdentificationDSDOnly getResource() {
        return resource;
    }

    public void setResource(MeIdentificationDSDOnly resource) {
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
