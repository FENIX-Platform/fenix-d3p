package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.Query;
import org.fao.fenix.d3p.process.dto.QueryParameter;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.*;

@ProcessName("select")
public class Select extends org.fao.fenix.d3p.process.Process<Query> {
    private @Inject Resources resourcesService;
    private @Inject StepFactory stepFactory;

    @Override
    public Step process(Query params, Step... sourceStep) throws Exception {
        //Retrieve source informations
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("Select filter can be applied only on a table or an other select query");
        String sourceData = type==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();
        //Update dsd before query creation
        filter(dsd,params.getColumns());
        //Create query
        String query = createQuery(params.getQuery(), dsd, sourceData);
        //Update query parameters
        Object[] existingParams = type==StepType.query ? ((QueryStep)source).getParams() : null;
        Collection<Object> queryParameters = existingParams!=null && existingParams.length>0 ? new LinkedList<>(Arrays.asList(existingParams)) : new LinkedList<>();
        Integer[] existingTypes = type==StepType.query ? ((QueryStep)source).getTypes() : null;
        Collection<Integer> queryTypes = existingTypes!=null && existingTypes.length>0 ? new LinkedList<>(Arrays.asList(existingTypes)) : null;

        Collection<QueryParameter> newQueryParameters = params.getQueryParameters();
        if (newQueryParameters!=null && newQueryParameters.size()>0) {
            Collection<Integer> queryParamsType = new LinkedList<>();
            boolean containsType = false;

            for (QueryParameter p : newQueryParameters) {
                //type
                Integer pType = p.jdbcType();
                containsType |= pType != null;
                queryParamsType.add(pType);
                //value
                queryParameters.add(p.getValue());
            }
            if (containsType && existingTypes!=null && existingTypes.length>0)
                queryTypes.addAll(queryParamsType);
        }
        //Create query step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(query);
        step.setParams(queryParameters.toArray());
        step.setTypes(queryTypes!=null && queryTypes.size()>0 ? queryTypes.toArray(new Integer[queryTypes.size()]) : null);

        return step;
    }


    //DSD adjustment (not distinct values)
    private void filter(DSDDataset dsd, Collection<String> columns) throws Exception {
        if (columns!=null && columns.size()>0) {
            Collection<DSDColumn> dsdColumns = new LinkedList<>();
            for (String columnId : columns)
                dsdColumns.add(dsd.findColumn(columnId));
            dsd.setColumns(dsdColumns);
        }
    }


    //QUERY UTILS
    private String createQuery(String paramQuery, DSDDataset dsd, String source) throws Exception {
        StringBuilder query = new StringBuilder();
        //Prepare select-from section
        String paramQueryLowerCase = paramQuery!=null ? paramQuery.trim().toLowerCase() : "";
        if (paramQueryLowerCase.indexOf("select")!=0) {
            query = new StringBuilder("SELECT ");
            for (DSDColumn column : dsd.getColumns())
                query.append(column.getId()).append(',');
            if (paramQueryLowerCase.indexOf("from")!=0) {
                query.setLength(query.length() - 1);
                query.append(" FROM ").append(source);
            }
            query.append(' ');
        }
        //Return query
        return query.append(paramQuery!=null ? paramQuery : "").toString();
    }

}
