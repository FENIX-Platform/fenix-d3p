package org.fao.fenix.d3p.process.dto;

import org.fao.fenix.d3p.process.type.UnionUsing;

public class UnionJoin {

    public static final UnionUsing DEFAULT_JOIN_TYPE = UnionUsing.subjectFirst;

    private UnionUsing using = UnionUsing.subjectOnly;
    private String[] columns;

    public UnionUsing getUsing() {
        return using!=null ? using : DEFAULT_JOIN_TYPE;
    }

    public void setUsing(UnionUsing using) {
        this.using = using;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }
}
