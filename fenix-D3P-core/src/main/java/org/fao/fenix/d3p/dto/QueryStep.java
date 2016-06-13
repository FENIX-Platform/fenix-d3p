package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.process.ProcessFactory;

import javax.inject.Inject;
import javax.ws.rs.NotAcceptableException;
import java.sql.*;
import java.util.Iterator;

public class QueryStep extends Step<String> {
    private Object[] params;
    private Integer[] types;

    private @Inject DatabaseUtils databaseUtils;

    @Override
    public StepType getType() {
        return StepType.query;
    }

    @Override
    public Iterator<Object[]> getData(Connection connection) throws Exception {
        PreparedStatement statement = connection.prepareStatement(getData());
        if(ProcessFactory.getTimeout()!= null)
            statement.setQueryTimeout(ProcessFactory.getTimeout());
        ResultSet rawData = null;
        try {
            rawData = databaseUtils.fillStatement(statement, getTypes(), getParams()).executeQuery();
        } catch (SQLException ex) {
            if( ex.getSQLState().equals("57014"))
                throw new  NotAcceptableException();
            throw new SQLException(ex);
        }
        return new DataIterator(rawData, null, null, null);
    }

    public Object[] getParams() {
        return params;
    }

    public void setParams(Object[] params) {
        this.params = params;
    }

    public Integer[] getTypes() {
        return types;
    }

    public void setTypes(Integer[] types) {
        this.types = types;
    }

}
