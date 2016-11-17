package org.fao.fenix.d3p.process.dto;


import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Query {
    private String query;
    private Collection<QueryParameter> queryParameters;
    private Boolean update;
    private Map<String,String> values = new HashMap<>();

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

    public Map<String, String> getValues() {
        return values;
    }

    public void setValues(Map<String, String> values) {
        this.values = values != null ? values : new HashMap<String,String>();
    }
}
