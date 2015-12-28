package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.*;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.OjCodeList;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Column;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.dto.dataset.Type;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.*;

@ProcessName("filter")
public class Filter extends org.fao.fenix.d3p.process.Process<DataFilter> {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;

    @Override
    public Step process(Connection connection, DataFilter params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("query filter can be applied only on a table or an other select query");
        String tableName = source!=null ? (String)source.getData() : null;
        DSDDataset dsd = source!=null ? source.getDsd() : null;
        if (tableName!=null && dsd!=null) {
            //Append label aggregations if needed
            Collection<String> columnsName = params.getColumns();
            Language[] languages = DatabaseStandards.getLanguageInfo();
            if (languages!=null && languages.length>0 && columnsName!=null && columnsName.size()>0)
                for (DSDColumn column : dsd.getColumns())
                    if ((column.getDataType()== DataType.code || column.getDataType()==DataType.customCode) && columnsName.contains(column.getId()))
                        for (Language l : languages) {
                            String id = column.getId() + '_' + l.getCode();
                            if (!columnsName.contains(id))
                                columnsName.add(id);
                        }
            //Normalize table name
            tableName = type==StepType.table ? tableName : '('+tableName+')';
            //Create query
            Object[] existingParams = type==StepType.query ? ((QueryStep)source).getParams() : null;
            Collection<Object> queryParameters = existingParams!=null && existingParams.length>0 ? new LinkedList<>(Arrays.asList(existingParams)) : new LinkedList<>();

            String query = createCacheFilterQuery(null, params, new Table(tableName, dsd), queryParameters, dsd.getColumns());
            //Generate and return query step
            QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
            step.setDsd(filter(dsd, params));
            step.setData(query);
            step.setParams(queryParameters.toArray());
            return step;
        } else
            throw new Exception ("Source step for data filtering is unavailable or incomplete.");
    }

    private DSDDataset filter (DSDDataset source, DataFilter filter) {
        DSDDataset dsd = new DSDDataset();
        dsd.setAggregationRules(source.getAggregationRules());
        dsd.setContextSystem("D3P");

        boolean removeKey = false;
        Collection<String> columnsName = filter.getColumns();
        if (columnsName!=null && columnsName.size()>0) {
            Collection<DSDColumn> columns = new LinkedList<>();
            for (DSDColumn column : source.getColumns())
                if (columnsName.contains(column.getId()))
                    columns.add(column);
                else
                    removeKey |= column.getKey()!=null && column.getKey();
            if (removeKey)
                for (DSDColumn column : columns)
                    column.setKey(false);
            dsd.setColumns(columns);
        } else
            dsd.setColumns(source.getColumns());
        return dsd;
    }

}


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
