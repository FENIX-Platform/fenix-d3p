package org.fao.fenix.d3p.process.dto;


public class JoinParameter {

    public static final JoinValueTypes DEFAULT_JOIN_TYPE = JoinValueTypes.id;

    private JoinValueTypes type;
    private Object value;
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public JoinValueTypes getType() {
        return type;
    }

    public void setType(JoinValueTypes type) {
        this.type = type!= null ? type: DEFAULT_JOIN_TYPE;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

}
