package org.fao.fenix.d3p.process.dto;



public class JoinParams {

    private String logic;

    private JoinParameter[][] joins;

    private String[][] values;

    public JoinParameter[][] getJoins() {
        return joins;
    }

    public void setJoins(JoinParameter[][] joins) {
        this.joins = joins;
    }

    public String[][] getValues() {
        return values;
    }

    public void setValues(String[][] values) {
        this.values = values;
    }

    public String getLogic() {
        return logic;
    }

    public void setLogic(String logic) {
        this.logic = logic;
    }
}
