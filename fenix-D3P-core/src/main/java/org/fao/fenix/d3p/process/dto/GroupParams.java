package org.fao.fenix.d3p.process.dto;

public class GroupParams {
    private String[] by = new String[0];
    private Aggregation[] aggregations = new Aggregation[0];


    public String[] getBy() {
        return by;
    }

    public void setBy(String[] by) {
        this.by = by;
    }

    public Aggregation[] getAggregations() {
        return aggregations;
    }

    public void setAggregations(Aggregation[] aggregations) {
        this.aggregations = aggregations;
    }
}
