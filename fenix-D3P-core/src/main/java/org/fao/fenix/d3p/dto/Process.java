package org.fao.fenix.d3p.dto;

import java.util.Map;

public class Process {

    private String name;
    private Map<String,Object> parameters;



    public Process() {
    }

    public Process(String name) {
        this.name = name;
    }

    public Process(String name, Map<String,Object> parameters) {
        this.name = name;
        this.parameters = parameters;
    }



    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String,Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String,Object> parameters) {
        this.parameters = parameters;
    }
}
