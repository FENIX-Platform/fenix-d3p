package org.fao.fenix.d3p.process.dto;

public class UnionParams {

    private String logic;
    private UnionJoin[] join;


    public String getLogic() {
        return logic;
    }

    public void setLogic(String logic) {
        this.logic = logic;
    }

    public UnionJoin[] getJoin() {
        return join;
    }

    public void setJoin(UnionJoin[] join) {
        this.join = join;
    }
}
