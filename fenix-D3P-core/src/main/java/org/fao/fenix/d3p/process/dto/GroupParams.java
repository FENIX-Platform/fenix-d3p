package org.fao.fenix.d3p.process.dto;

public class GroupParams {
    private String[] by;
    private Aggregation[] aggregations;


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
