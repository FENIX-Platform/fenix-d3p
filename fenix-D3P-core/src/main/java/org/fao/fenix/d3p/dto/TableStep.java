package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.templates.identification.DSDCodelist;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;

public class TableStep extends Step<String> {
    @Inject DatabaseUtils databaseUtils;

    @Override
    public StepType getType() {
        return StepType.table;
    }

    @Override
    public Iterator<Object[]> getData(Connection connection) throws Exception {
        ResultSet rawData = connection.createStatement().executeQuery(getQuery());
        return new DataIterator(rawData, null, null, null);
    }

    //Utils
    public QueryStep toQueryStep() {
        QueryStep step = new QueryStep();
        step.setRid(getRid());
        step.setDsd(getDsd());
        step.setData(getQuery());
        return step;
    }

    public String getQuery() {
        StringBuilder query = new StringBuilder();
        for (DSDColumn column : getDsd().getColumns())
            query.append(',').append(column.getId());
        return "select "+query.substring(1)+" from "+getData();
    }

}
