package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.*;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.dto.SimpleFilterParams;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Column;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.dto.dataset.Type;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.Types;
import java.util.*;

@ProcessName("simpleFilter")
public class SimpleFilter extends org.fao.fenix.d3p.process.Process<SimpleFilterParams> {
    private @Inject StepFactory stepFactory;

    @Override
    public Step process(Connection connection, SimpleFilterParams params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 && sourceStep[0] instanceof TableStep ? (TableStep)sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("query filter can be applied only on a table or an other select query");
        String tableName = source!=null ? (String)source.getData() : null;
        DSDDataset dsd = source!=null ? source.getDsd() : null;
        if (tableName!=null && dsd!=null) {
            DataFilter filter = params!=null ? params.getFilter() : null;
            Order order = params!=null ? params.getOrder() : null;
            //Append label aggregations if needed
            Collection<String> columnsName = filter!=null ? filter.getColumns() : null;
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
            tableName = type==StepType.table ? tableName : '('+tableName+") as " + source.getRid();
            //Create query
            Object[] existingParams = type==StepType.query ? ((QueryStep)source).getParams() : null;
            Collection<Object> queryParameters = existingParams!=null && existingParams.length>0 ? new LinkedList<>(Arrays.asList(existingParams)) : new LinkedList<>();
            Integer[] existingTypes = type==StepType.query ? ((QueryStep)source).getTypes() : null;
            Collection<Integer> queryTypes = existingTypes!=null && existingTypes.length>0 ? new LinkedList<>(Arrays.asList(existingTypes)) : null;

            String query = createCacheFilterQuery(null, params.getFilter(), new Table(tableName, dsd), queryParameters, queryTypes, dsd.getColumns());
            //Add ordering
            if (order!=null)
                if (columnsName!=null && columnsName.size()>0)
                    query+=order.toH2SQL(columnsName.toArray(new String[columnsName.size()]));
                else
                    query+=order.toH2SQL();
            //Generate and return query step
            QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
            step.setDsd(filter(dsd, params.getFilter()));
            step.setData(query);
            step.setParams(queryParameters.toArray());
            step.setTypes(queryTypes!=null && queryTypes.size()>0 ? queryTypes.toArray(new Integer[queryTypes.size()]) : null);
            step.setRid(getRandomTmpTableName());
            return step;
        } else
            throw new Exception ("Source step for data filtering is unavailable or incomplete.");
    }

    private DSDDataset filter (DSDDataset source, DataFilter filter) {
        DSDDataset dsd = new DSDDataset();
        dsd.setAggregationRules(source.getAggregationRules());
        dsd.setContextSystem("D3P");

        Collection<String> columnsName = filter!=null ? filter.getColumns() : null;
        if (columnsName!=null && columnsName.size()>0) {
            Collection<DSDColumn> columns = new LinkedList<>();
            for (DSDColumn column : source.getColumns())
                if (columnsName.contains(column.getId()))
                    columns.add(column);
                else if (column.getKey())
                    throw new UnsupportedOperationException("Cannot remove key columns from selection");
            dsd.setColumns(columns);
        } else
            dsd.setColumns(source.getColumns());
        return dsd;
    }

}
