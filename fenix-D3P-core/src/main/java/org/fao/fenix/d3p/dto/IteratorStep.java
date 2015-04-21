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
    public Collection<Object[]> getData(Connection connection) throws Exception {
        Collection<Object[]> data = new LinkedList<>();
        for (Iterator<Object[]> rawData = getData(); rawData!=null && rawData.hasNext(); )
            data.add(rawData.next());
        return data;
    }


}
