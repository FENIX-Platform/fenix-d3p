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
    public Step process(Connection connection, Query params, Step... sourceStep) throws Exception {
        //Retrieve source informations
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("Select filter can be applied only on a table or an other select query");
        String sourceData = (String)source.getData();
        DSDDataset dsd = source.getDsd();
        //Return correspondent "query" step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(filter(dsd,params.getColumns())); //DSD adjustment before query creation
        step.setData(createQuery(params.getQuery(), dsd, sourceData));
        //Set query parameters
        Collection<QueryParameter> queryParameters = params.getQueryParameters();
        if (queryParameters!=null && queryParameters.size()>0) {
            Object[] queryParamsValue = new Object[queryParameters.size()];
            Integer[] queryParamsType = new Integer[queryParameters.size()];
            boolean containsType = false;

            Iterator<QueryParameter> queryParameterIterator = queryParameters.iterator();
            for (int i=0; i<queryParamsValue.length; i++) {
                QueryParameter p = queryParameterIterator.next();
                //type
                Integer pType = p.jdbcType();
                containsType |= pType != null;
                queryParamsType[i] = pType;
                //value
                queryParamsValue[i] = p.getValue();
            }

            step.setParams(queryParamsValue);
            step.setTypes(containsType ? queryParamsType : null);
        }
        step.setRid(getRandomTmpTableName());
        return step;
    }


    //DSD adjustment (non distinct values)
    private DSDDataset filter(DSDDataset dsd, Collection<String> columns) throws Exception {
        if (columns!=null && columns.size()>0) {
            Collection<DSDColumn> dsdColumns = new LinkedList<>();
            for (String columnId : columns)
                dsdColumns.add(dsd.findColumn(columnId));
            dsd.setColumns(dsdColumns);
        }
        return dsd;
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
