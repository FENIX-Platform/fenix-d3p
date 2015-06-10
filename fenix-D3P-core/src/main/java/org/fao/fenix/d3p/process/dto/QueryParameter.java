package org.fao.fenix.d3p.process.dto;

import java.sql.Types;

public class QueryParameter {
    private Object value;
    private String type;

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    //Utils
    public Integer jdbcType() throws NoSuchFieldException, IllegalAccessException {
        return type!=null ? Types.class.getField(type.toUpperCase()).getInt(null) : null;
    }
}
