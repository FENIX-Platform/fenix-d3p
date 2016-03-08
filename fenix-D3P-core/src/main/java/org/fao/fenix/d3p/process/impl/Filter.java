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
    public Step process(DataFilter params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("filter process can be applied only on a table or an other select query");
        String tableName = type==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();
        //Add label columns if needed
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
        tableName = type==StepType.table ? tableName : '('+tableName+") as " + source.getRid();
        //Create query
        Object[] existingParams = type==StepType.query ? ((QueryStep)source).getParams() : null;
        Collection<Object> queryParameters = existingParams!=null && existingParams.length>0 ? new LinkedList<>(Arrays.asList(existingParams)) : new LinkedList<>();
        Integer[] existingTypes = type==StepType.query ? ((QueryStep)source).getTypes() : null;
        Collection<Integer> queryTypes = existingTypes!=null && existingTypes.length>0 ? new LinkedList<>(Arrays.asList(existingTypes)) : null;

        String query = createCacheFilterQuery(null, params, new Table(tableName, dsd), queryParameters, queryTypes, dsd.getColumns());
        //Update dsd
        updateDsd(dsd, params);
        //Generate and return query step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(query);
        step.setParams(queryParameters.toArray());
        step.setTypes(queryTypes!=null && queryTypes.size()>0 ? queryTypes.toArray(new Integer[queryTypes.size()]) : null);
        return step;
    }

    private void updateDsd (DSDDataset dsd, DataFilter filter) {
        boolean removeKey = false;
        Collection<String> columnsName = filter.getColumns();
        if (columnsName!=null && columnsName.size()>0) {
            Collection<DSDColumn> columns = new LinkedList<>();
            for (DSDColumn column : dsd.getColumns())
                if (columnsName.contains(column.getId()))
                    columns.add(column);
                else
                    removeKey |= column.getKey()!=null && column.getKey();
            if (removeKey)
                for (DSDColumn column : columns)
                    column.setKey(false);
            dsd.setColumns(columns);
        }
    }

}

