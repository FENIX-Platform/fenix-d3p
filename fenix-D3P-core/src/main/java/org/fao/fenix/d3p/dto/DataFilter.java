package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.find.dto.filter.StandardFilter;

import java.util.Collection;

public class DataFilter {
    private StandardFilter rows;
    private Collection<String> columns;


    public StandardFilter getRows() {
        return rows;
    }

    public void setRows(StandardFilter rows) {
        this.rows = rows;
    }

    public Collection<String> getColumns() {
        return columns;
    }

    public void setColumns(Collection<String> columns) {
        this.columns = columns;
    }

}
