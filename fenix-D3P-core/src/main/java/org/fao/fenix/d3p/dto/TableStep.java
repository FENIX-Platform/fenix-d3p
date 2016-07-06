package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3p.process.ProcessFactory;

import javax.inject.Inject;
import javax.ws.rs.NotAcceptableException;
import java.sql.*;
import java.util.Iterator;

public class TableStep extends Step<String> {
    private
    @Inject
    DatabaseUtils databaseUtils;
    private
    @Inject
    CacheFactory cacheFactory;


    @Override
    public StepType getType() {
        return StepType.table;
    }

    @Override
    public Iterator<Object[]> getData(Connection connection) throws Exception {
        Statement statement = connection.createStatement();
        if (ProcessFactory.getTimeout() != null)
            statement.setQueryTimeout(ProcessFactory.getTimeout());
        ResultSet rawData = null;
        try {
            rawData = statement.executeQuery(getQuery());
        } catch (SQLException ex) {
            if (ex.getSQLState().equals("57014"))
                throw new NotAcceptableException();
        }
        return new DataIterator(rawData, connection, null, null);
    }

    //Utils
    public QueryStep toQueryStep() throws Exception {
        QueryStep step = new QueryStep();
        step.setRid(getRid());
        step.setDsd(getDsd());
        step.setData(getQuery());
        return step;
    }

    public String getQuery() throws Exception {
        StringBuilder query = new StringBuilder();
        for (DSDColumn column : getDsd().getColumns())
            query.append(',').append(column.getId());
        return "select " + query.substring(1) + " from " + getData();
    }

}
