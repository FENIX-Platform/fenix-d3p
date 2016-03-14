package org.fao.fenix.d3p.process.dto;

public class UnionParams {

    private String logic;
    private boolean label = false;
    private UnionJoin[] join;


    public String getLogic() {
        return logic;
    }

    public void setLogic(String logic) {
        this.logic = logic;
    }

    public boolean isLabel() {
        return label;
    }

    public void setLabel(boolean label) {
        this.label = label;
    }

    public UnionJoin[] getJoin() {
        return join;
    }

    public void setJoin(UnionJoin[] join) {
        this.join = join;
    }
}
