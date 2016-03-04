package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;

import java.sql.Connection;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class IteratorStep extends Step<Iterator<Object[]>> {

    @Override
    public StepType getType() {
        return StepType.iterator;
    }

    @Override
    public Iterator<Object[]> getData(Connection connection) throws Exception {
        return getData();
    }

    //Prevent more than one iterator consumption
    private boolean containsData = false;

    @Override
    public void setData(Iterator<Object[]> data) {
        super.setData(data);
        containsData = data.hasNext();
    }

    @Override
    public Iterator<Object[]> getData() {
        Iterator<Object[]> currentData = super.getData();
        if (containsData && !currentData.hasNext())
            throw new NoDataException();
        return super.getData();
    }
}
