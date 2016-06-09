package org.fao.fenix.d3p.process.dto;


import org.fao.fenix.commons.msd.dto.full.DSDColumn;

public class AddColumnParams {

    private DSDColumn column;
    private Object value;

    public DSDColumn getColumn() {
        return column;
    }

    public void setColumn(DSDColumn column) {
        this.column = column;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
