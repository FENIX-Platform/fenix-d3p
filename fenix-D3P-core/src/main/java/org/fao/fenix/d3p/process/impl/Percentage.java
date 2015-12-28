package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.*;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.OjCodeList;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Column;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.dto.dataset.Type;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.inject.Inject;
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
    public Step process(Connection connection, PercentageFilter params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("Percentage filter can be applied only on a table or an other select query");
        String tableName = source!=null ? (String)source.getData() : null;
        DSDDataset dsd = source!=null ? source.getDsd() : null;
        DSDColumn valueColumn = getValueColumn(dsd);
        Collection<String> keyColumnsId = getKeyColumnsId(dsd);

        if (tableName==null || valueColumn==null || keyColumnsId==null)
            throw new Exception ("Source step for data percentage calculation is unavailable or incomplete or without a number value column.");

        String valueColumnId = valueColumn.getId();

        //Retrieve source info
        tableName = type==StepType.table ? tableName : '('+tableName+')';
        Object[] existingParams = type==StepType.query ? ((QueryStep)source).getParams() : null;

        //Define mode
        Double total = params!=null ? params.getTotal() : null;
        if (total==null && (params==null || params.getTotalRows()==null || params.getTotalRows().size()==0)) { //Mode 2 prefetch
            //Select total value
            ResultSet rawData = databaseUtils.fillStatement(connection.prepareStatement("SELECT SUM("+valueColumnId+") FROM "+tableName), null, existingParams).executeQuery();
            total = rawData.next() ? rawData.getDouble(1) : 0;
        }

        //Apply
        if (total==null) { //Apply mode 3 (only from table)
            if (type!=StepType.table)
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
            String query = createCacheFilterQuery(null, totalsFilter, new Table(tableName, dsd), queryParameters, dsd.getColumns());
            ResultSet totalsRawData = databaseUtils.fillStatement(connection.prepareStatement(query), null, queryParameters.toArray()).executeQuery();

            //Update table
            Collection<String> dataKeyColumns = new LinkedHashSet<>(keyColumnsId);
            dataKeyColumns.removeAll(params.getTotalRows().keySet());
            while (totalsRawData.next()) {
                Collection<Object> keyParams = new LinkedList<>();
                for (String totalSelectColumnId : dataKeyColumns)
                    keyParams.add(totalsRawData.getObject(totalSelectColumnId));
                //Run update query
                PreparedStatement updateStatement = connection.prepareStatement(createUpdateQuery(totalsRawData.getDouble(valueColumnId),valueColumnId,tableName,dataKeyColumns));
                databaseUtils.fillStatement(updateStatement, null, keyParams.toArray()).executeUpdate();
            }

            //Run delete of totals if needed
            if (!params.isInclusive()) {
                queryParameters = new LinkedList<>();
                query = createCacheDeleteQuery(params.getTotalRows(), new Table(tableName, dsd), queryParameters, dsd.getColumns());
                databaseUtils.fillStatement(connection.prepareStatement(query), null, queryParameters.toArray()).executeUpdate();
            }

            //Return source step
            return source;
        } else { //Apply mode 1 or 2
            //Replace value column id with formula
            valueColumn.setId(getValueSelect(total, valueColumnId)+" as "+valueColumnId);
            //Prepare query
            Collection<Object> queryParameters = existingParams!=null && existingParams.length>0 ? new LinkedList<>(Arrays.asList(existingParams)) : new LinkedList<>();
            String query = createCacheFilterQuery(null, null, new Table(tableName, dsd), queryParameters, dsd.getColumns());
            //Restore value column id
            valueColumn.setId(valueColumnId);
            //Create and return query step
            QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
            step.setDsd(dsd);
            step.setData(query);
            step.setParams(queryParameters.toArray());
            return step;
        }
    }


    //Utils

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

//Append label aggregations if needed
/*            Collection<String> columnsName = params.getKey();
            Language[] languages = DatabaseStandards.getLanguageInfo();
            if (languages!=null && languages.length>0 && columnsName!=null && columnsName.size()>0)
                for (DSDColumn column : dsd.getColumns())
                    if ((column.getDataType()== DataType.code || column.getDataType()==DataType.customCode) && columnsName.contains(column.getId()))
                        for (Language l : languages) {
                            String id = column.getId() + '_' + l.getCode();
                            if (!columnsName.contains(id))
                                columnsName.add(id);
                        }
*/

/*
    private int[] getSQLTypes (DSDDataset dsd) {
        if (dsd!=null && dsd.getColumns()!=null) {
            Table tmpTable = new Table("tmp", dsd);
            int[] types = new int[tmpTable.getColumns().size()];
            Iterator<Column> columns = tmpTable.getColumns().iterator();
            for (int i=0; i<types.length; i++)
                switch (columns.next().getType()) {
                    case bool:      types[i] = Types.BOOLEAN; break;
                    case real:      types[i] = Types.DOUBLE; break;
                    case string:    types[i] = Types.VARCHAR; break;
                    case array:     types[i] = Types.ARRAY; break;
                    case object:    types[i] = Types.OTHER; break;
                    case integer:   types[i] = Types.BIGINT; break;
                }
            return types;
        } else
            return null;
    }
*/

/*
    private DSDDataset filter (DSDDataset source, DataFilter filter) {
        DSDDataset dsd = new DSDDataset();
        dsd.setAggregationRules(source.getAggregationRules());
        dsd.setContextSystem("D3P");

        Collection<String> columnsName = filter.getColumns();
        if (columnsName!=null && columnsName.size()>0) {
            Collection<DSDColumn> columns = new LinkedList<>();
            for (DSDColumn column : source.getColumns())
                if (columnsName.contains(column.getId()))
                    columns.add(column);
                else if (column.getKey()!=null && column.getKey())
                    throw new UnsupportedOperationException("Cannot remove key columns from selection");
            dsd.setColumns(columns);
        } else
            dsd.setColumns(source.getColumns());
        return dsd;
    }
*/
