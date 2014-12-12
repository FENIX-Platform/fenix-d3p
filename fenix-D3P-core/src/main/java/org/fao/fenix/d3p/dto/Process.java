package org.fao.fenix.d3p.dto;

public class Process<T> {

    private String name;
    private T parameters;



    public Process() {
    }

    public Process(String name) {
        this.name = name;
    }

    public Process(String name, T parameters) {
        this.name = name;
        this.parameters = parameters;
    }



    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getParameters() {
        return parameters;
    }

    public void setParameters(T parameters) {
        this.parameters = parameters;
    }
}
