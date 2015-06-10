package org.fao.fenix.d3p.process.dto;


import java.util.Collection;

public class Query {
    private String query;
    private Collection<QueryParameter> queryParameters;
    private Boolean update;
    private Collection<String> columns;


    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Collection<QueryParameter> getQueryParameters() {
        return queryParameters;
    }

    public void setQueryParameters(Collection<QueryParameter> queryParameters) {
        this.queryParameters = queryParameters;
    }

    public Boolean getUpdate() {
        return update;
    }

    public void setUpdate(Boolean update) {
        this.update = update;
    }

    public Collection<String> getColumns() {
        return columns;
    }

    public void setColumns(Collection<String> columns) {
        this.columns = columns;
    }
}
