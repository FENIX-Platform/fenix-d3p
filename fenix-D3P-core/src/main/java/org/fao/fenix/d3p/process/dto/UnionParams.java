package org.fao.fenix.d3p.process.dto;

public class UnionParams {

    private UnionMode mode = UnionMode.intersect;
    private boolean label = false;
    private UnionJoin[] join;


    public UnionMode getMode() {
        return mode;
    }

    public void setMode(UnionMode mode) {
        this.mode = mode;
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
