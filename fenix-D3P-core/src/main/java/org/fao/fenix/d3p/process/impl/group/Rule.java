package org.fao.fenix.d3p.process.impl.group;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.h2.api.AggregateFunction;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

public abstract class Rule implements AggregateFunction {
    private String[] columns;
    private Map<String,String> config;
    private Map<String,Object> params;
    private DSDDataset dsd;


    @Override
    public final void add(Object o) throws SQLException {
        Object[] values = (Object[])o;
        if (source==null)
            source = RulesFactory.getRule((String) values[0]);

        append(Arrays.copyOfRange(values, 1,values.length));
    }

    protected abstract void append(Object[] values) throws SQLException;

    private Rule source;

    protected <T extends Rule> T getSource(T extension) {
        return (T)source;
    }

    public String[] getColumns() {
        return source==null ? columns : source.getColumns();
    }

    public void setColumns(String[] columns) {
        if (source==null)
            this.columns = columns;
        else
            source.setColumns(columns);
    }

    public Map<String, String> getConfig() {
        return source==null ? config : source.getConfig();
    }

    public void setConfig(Map<String, String> config) {
        if (source==null)
            this.config = config;
        else
            source.setConfig(config);
    }

    public Map<String, Object> getParams() {
        return source==null ? params : source.getParams();
    }

    public void setParams(Map<String, Object> params) {
        if (source==null)
            this.params = params;
        else
            source.setParams(params);
    }

    public DSDDataset getDsd() {
        return source== null ? dsd : source.getDsd();
    }

    public void setDsd(DSDDataset dsd) {
        if (source==null)
            this.dsd = dsd;
        else
            source.setDsd(dsd);
    }
}
