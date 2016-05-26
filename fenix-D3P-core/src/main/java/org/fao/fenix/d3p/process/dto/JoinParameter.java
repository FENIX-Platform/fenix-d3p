package org.fao.fenix.d3p.process.dto;


public class JoinParameter {

    public static final JoinTypes DEFAULT_JOIN_TYPE = JoinTypes.id;

    private JoinTypes type;
    private Object value;
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public JoinTypes getType() {
        return type;
    }

    public void setType(JoinTypes type) {
        this.type = type!= null ? type: DEFAULT_JOIN_TYPE;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

}
