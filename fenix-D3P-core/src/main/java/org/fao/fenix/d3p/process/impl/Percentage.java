package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.*;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

@ProcessName("percentage")
public class Percentage extends org.fao.fenix.d3p.process.Process<PercentageFilter> {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;
    private @Inject Resources resourcesService;

    @Override
    public Step process(PercentageFilter params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length>0 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("Percentage filter can be applied only on a table or an other select query");
        String tableName = type==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();
        DSDColumn valueColumn = getValueColumn(dsd);
        Collection<String> keyColumnsId = getKeyColumnsId(dsd);

        if (tableName==null || valueColumn==null || keyColumnsId==null)
            throw new BadRequestException("Source step for data percentage calculation is unavailable or incomplete or without a number value column.");

        String valueColumnId = valueColumn.getId();

        //Retrieve source info
        Object[] existingParams = type==StepType.query ? ((QueryStep)source).getParams() : null;
        Integer[] existingTypes = type==StepType.query ? ((QueryStep)source).getTypes() : null;

        //Logic
        Connection connection = source.getStorage().getConnection();
        try {
            //Define mode
            Double total = params != null ? params.getTotal() : null;
            if (total == null && (params == null || params.getTotalRows() == null || params.getTotalRows().size() == 0)) { //Mode 2 prefetch
                //Select total value
                ResultSet rawData = databaseUtils.fillStatement(connection.prepareStatement("SELECT SUM(" + valueColumnId + ") FROM " + tableName), null, existingParams).executeQuery();
                total = rawData.next() ? rawData.getDouble(1) : 0;
            }

            //Apply
            if (total == null) { //Apply mode 3 (only from table)
                if (type != StepType.table)
                    throw new UnsupportedOperationException("Percentage filter in mode 3 can be applied only on a table");

                //TODO Verify table isn't a D3S table
                //resourcesService.loadMetadata()

                //Check rows filter validity: only key columns can be used
                for (String filterColumn : params.getTotalRows().keySet())
                    if (!keyColumnsId.contains(filterColumn))
                        throw new UnsupportedOperationException("Percentage filter in mode 3 can filter totals value only by using key columns");

                //Select totals
                DataFilter totalsFilter = new DataFilter();
                totalsFilter.setRows(params.getTotalRows());
                Collection<String> columns = new LinkedList<>(keyColumnsId);
                columns.add(valueColumnId);
                totalsFilter.setColumns(columns);

                Collection<Object> queryParameters = new LinkedList<>();
                String query = createCacheFilterQuery(null, totalsFilter, new Table(tableName, dsd), queryParameters, null, dsd.getColumns(), null);
                ResultSet totalsRawData = databaseUtils.fillStatement(connection.prepareStatement(query), null, queryParameters.toArray()).executeQuery();

                //Update table
                Collection<String> dataKeyColumns = new LinkedHashSet<>(keyColumnsId);
                dataKeyColumns.removeAll(params.getTotalRows().keySet());
                while (totalsRawData.next()) {
                    Collection<Object> keyParams = new LinkedList<>();
                    for (String totalSelectColumnId : dataKeyColumns)
                        keyParams.add(totalsRawData.getObject(totalSelectColumnId));
                    //Run update query
                    PreparedStatement updateStatement = connection.prepareStatement(createUpdateQuery(totalsRawData.getDouble(valueColumnId), valueColumnId, tableName, dataKeyColumns));
                    databaseUtils.fillStatement(updateStatement, null, keyParams.toArray()).executeUpdate();
                }

                //Run delete of totals if needed
                if (!params.isInclusive()) {
                    queryParameters = new LinkedList<>();
                    query = createCacheDeleteQuery(params.getTotalRows(), new Table(tableName, dsd), queryParameters, null, dsd.getColumns());
                    databaseUtils.fillStatement(connection.prepareStatement(query), null, queryParameters.toArray()).executeUpdate();
                }

                //Return source step
                return source;
            } else { //Apply mode 1 or 2
                //Create and return query step
                QueryStep step = (QueryStep) stepFactory.getInstance(StepType.query);
                step.setDsd(dsd);
                step.setData(createQuery(dsd, tableName, total));
                step.setParams(existingParams);
                step.setTypes(existingTypes);
                return step;
            }
        } finally {
            connection.close();
        }
    }


    //Utils

    private String createQuery(DSDDataset dsd, String tableName, double total) {
        StringBuilder query = new StringBuilder("SELECT ");
        for (DSDColumn column : dsd.getColumns())
            if ("value".equals(column.getSubject()))
                query.append(getValueSelect(total, column.getId())).append(" AS ").append(column.getId()).append(',');
            else
                query.append(column.getId()).append(',');
        query.setLength(query.length()-1);
        return query.append(" FROM ").append(tableName).toString();
    }

    private String getValueSelect (double total, String valueColumnId) {
        return valueColumnId+"*100/"+total;
    }

    private String getKeyWhereCondition(Collection<String> filterColumns) {
        StringBuilder condition = new StringBuilder();
        for (String columnId : filterColumns)
            condition.append(" AND ").append(columnId).append(" = ?");
        return condition.length()>0 ? condition.substring(5) : null;
    }

    private Collection<String> getKeyColumnsId(DSDDataset dsd) {
        Collection<String> keyColumns = new LinkedHashSet<>();
        if (dsd!=null)
            for (DSDColumn column : dsd.getColumns())
                if (column.getKey()!=null && column.getKey())
                    keyColumns.add(column.getId());
        return keyColumns.size()>0 ? keyColumns : null;
    }

    private String createUpdateQuery(double total, String valueColumnId, String tableName, Collection<String> filterColumns) {
        String whereCondition = getKeyWhereCondition(filterColumns);
        return new StringBuilder("UPDATE ")
                .append(tableName)
                .append(" SET ")
                .append(valueColumnId)
                .append(" = ")
                .append(getValueSelect(total, valueColumnId))
                .append(whereCondition!=null ? " WHERE "+whereCondition : "")
                .toString();
    }

    private DSDColumn getValueColumn(DSDDataset dsd) {
        if (dsd!=null && dsd.getColumns()!=null)
            for (DSDColumn column : dsd.getColumns())
                if ("value".equals(column.getSubject()) && column.getDataType()==DataType.number)
                    return column;
        return null;
    }


}
