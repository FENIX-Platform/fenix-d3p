package org.fao.fenix.d3p.dto;

import java.util.Iterator;

public class IteratorStep extends Step<Iterator<Object[]>> {

    @Override
    public StepType getType() {
        return StepType.iterator;
    }
}
