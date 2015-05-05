package org.fao.fenix.d3p.process.impl.group;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.d3p.process.impl.QueryGroup;

import java.sql.Connection;
import java.util.Map;

public abstract class Rule {
    private String[] columns;
    private Map<String,String> config;
    private Map<String,Object> params;
    private DSDDataset dsd;


    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public DSDDataset getDsd() {
        return dsd;
    }

    public void setDsd(DSDDataset dsd) {
        this.dsd = dsd;
    }
}
