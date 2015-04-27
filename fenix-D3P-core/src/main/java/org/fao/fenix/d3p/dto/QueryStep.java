package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;

public class QueryStep extends Step<String> {
    private Object[] params;
    private int[] types;

    private @Inject DatabaseUtils databaseUtils;

    @Override
    public StepType getType() {
        return StepType.query;
    }

    @Override
    public Iterator<Object[]> getData(Connection connection) throws Exception {
        ResultSet rawData = databaseUtils.fillStatement(connection.prepareStatement(getData()), getTypes(), getParams()).executeQuery();
        return new DataIterator(rawData, null, null, null);
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
