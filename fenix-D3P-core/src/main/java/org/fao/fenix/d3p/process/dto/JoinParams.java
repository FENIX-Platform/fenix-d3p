package org.fao.fenix.d3p.process.dto;


import java.util.Collection;

public class JoinParams {

    private Collection<Collection<JoinParameter>> joins;

    private Collection<Collection<String>> values;

    public Collection<Collection<JoinParameter>> getJoins() {
        return joins;
    }

    public void setJoins(Collection<Collection<JoinParameter>> joins) {
        this.joins = joins;
    }

    public Collection<Collection<String>> getValues() {
        return values;
    }

    public void setValues(Collection<Collection<String>> values) {
        this.values = values;
    }
}
