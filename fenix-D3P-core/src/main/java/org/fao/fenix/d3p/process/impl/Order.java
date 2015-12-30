package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.Collection;

@ProcessName("order")
public class Order extends org.fao.fenix.d3p.process.Process<org.fao.fenix.commons.utils.Order> {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;

    @Override
    public Step process(Connection connection, org.fao.fenix.commons.utils.Order params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        String tableName = source!=null ? (String)source.getData() : null;
        if (tableName==null)
            throw new Exception("No data to apply ordering");

        StepType sourceType = source.getType();
        DSDDataset dsd = source.getDsd();
        Collection<DSDColumn> columns = dsd!=null ? dsd.getColumns() : null;
        if (sourceType==null || (sourceType!=StepType.table && sourceType!=StepType.query))
            throw new UnsupportedOperationException("order filter can be applied only on a table or an other select query");
        if (columns==null)
            throw new Exception("Cannot order source with undefined structure");

        String[] columnsName = new String[columns.size()];
        int i = 0;
        for (DSDColumn column : columns)
            columnsName[i++] = column.getId();


        //Normalize table name
        tableName = sourceType==StepType.table ? tableName : '('+tableName+") as " + source.getRid();
        //Create and update query
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(source.getDsd());
        step.setData(createOrderQuery(params, tableName, columnsName));
        if (sourceType==StepType.query) {
            step.setParams(((QueryStep) source).getParams());
            step.setTypes(((QueryStep) source).getTypes());
        }
        step.setRid(getRandomTmpTableName());

        return step;
    }


    private String createOrderQuery(org.fao.fenix.commons.utils.Order ordering, String from, String[] columnsName) throws Exception {
        StringBuilder query = new StringBuilder("SELECT * FROM ").append(from);
        if (ordering!=null)
            query.append(ordering.toH2SQL(columnsName));
        return query.toString();
    }

}
