package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.templates.identification.MeIdentification;

public class DomainStatus {
    MeIdentification identification;

    public MeIdentification getIdentification() {
        return identification;
    }

    public void setIdentification(MeIdentification identification) {
        this.identification = identification;
    }
}
