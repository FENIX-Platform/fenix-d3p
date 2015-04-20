package org.fao.fenix.d3p.dto;

public class QueryStep extends Step<String> {
    private boolean select;
    private Object[] params;
    private int[] types;


    @Override
    public StepType getType() {
        return StepType.query;
    }

    public boolean isSelect() {
        return select;
    }

    public void setSelect(boolean select) {
        this.select = select;
    }

    public Object[] getParams() {
        return params;
    }

    public void setParams(Object[] params) {
        this.params = params;
    }

    public int[] getTypes() {
        return types;
    }

    public void setTypes(int[] types) {
        this.types = types;
    }

}
