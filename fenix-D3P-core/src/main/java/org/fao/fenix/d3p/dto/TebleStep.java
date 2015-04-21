package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.templates.identification.DSDCodelist;
import org.fao.fenix.commons.utils.database.DatabaseUtils;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.Collection;

public class TebleStep extends Step<String> {
    @Inject DatabaseUtils databaseUtils;

    @Override
    public StepType getType() {
        return StepType.table;
    }

    @Override
    public Collection<Object[]> getData(Connection connection) throws Exception {
        return databaseUtils.getDataCollection(connection.createStatement().executeQuery(getQuery()));
    }

    //Utils
    public QueryStep toQueryStep() {
        QueryStep step = new QueryStep();
        step.setRid(getRid());
        step.setDsd(getDsd());
        step.setData(getQuery());
        return step;
    }

    private String getQuery() {
        StringBuilder query = new StringBuilder();
        for (DSDColumn column : getDsd().getColumns())
            query.append(',').append(column.getId());
        return "select "+query.substring(1)+" from "+getData();
    }

}
