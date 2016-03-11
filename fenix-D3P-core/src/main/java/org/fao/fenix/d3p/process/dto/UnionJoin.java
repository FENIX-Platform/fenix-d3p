package org.fao.fenix.d3p.process.dto;

public class UnionJoin {

    private UnionUsing using = UnionUsing.subjectOnly;
    private String[] columns;

    public UnionUsing getUsing() {
        return using;
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
