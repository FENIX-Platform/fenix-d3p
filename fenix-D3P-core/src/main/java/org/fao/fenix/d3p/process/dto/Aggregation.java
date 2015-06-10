package org.fao.fenix.d3p.process.dto;

import java.util.HashMap;
import java.util.Map;

public class Aggregation {
    private String[] columns;
    private String rule;
    private Map<String, Object> parameters = new HashMap<>();


    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    //Utils
    public String getCid() {
        return columns!=null && columns.length>0 ? columns[0] : null;
    }
}
