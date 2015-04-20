package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;

public abstract class Step<T> {
    private T data;
    private DSDDataset dsd;
    private String rid;


    public abstract StepType getType();

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public DSDDataset getDsd() {
        return dsd;
    }

    public void setDsd(DSDDataset dsd) {
        this.dsd = dsd;
    }

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }
}
