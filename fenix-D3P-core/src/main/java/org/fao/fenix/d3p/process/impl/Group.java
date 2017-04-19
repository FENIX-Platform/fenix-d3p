package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.DisposableProcess;
import org.fao.fenix.d3p.process.dto.Aggregation;
import org.fao.fenix.d3p.process.dto.GroupParams;
import org.fao.fenix.d3p.process.impl.group.RulesFactory;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.storage.dataset.h2.DefaultStorage;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import java.util.*;

@ProcessName("group")
public class Group extends DisposableProcess<GroupParams> {

    private @Inject RulesFactory rulesFactory;
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;
    private @Inject UIDUtils uidUtils;

    private String pid;

    @Override
    public void dispose() throws Exception {
        if (pid!=null)
            rulesFactory.delRule(pid);
    }

    @Override
    public Step process(GroupParams params, Step... sourceStep) throws Exception {
        //Retrieve source informations
        Step source = sourceStep!=null && sourceStep.length>0 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("group process support only one table or query input step");
        String sourceData = (String)source.getData();
        sourceData = type==StepType.table ? sourceData : '('+sourceData+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();
        Set<String> groupsKey = new LinkedHashSet<>(Arrays.asList(params.getBy()));
        //check for H2 availability
        boolean useDefaultStorage = source.getStorage() instanceof DefaultStorage;
        pid = useDefaultStorage ? uidUtils.getId() : null;
        //Append label aggregations if needed
        if (params.getAggregations()==null)
            params.setAggregations(new Aggregation[0]);
        Collection<Aggregation> aggregations = new LinkedList<>(Arrays.asList(params.getAggregations()));
        addLanguageColumnsAggregations(aggregations, params, dsd);
        //Define groups rule
        Map<String, String> groups = new HashMap<>();
        for (Aggregation aggregation : aggregations) {
            String ruleId = useDefaultStorage ? rulesFactory.setRule(aggregation.getRule(), pid, aggregation.getParameters(), dsd, aggregation.getColumns()) : null;
            groups.put(aggregation.getCid(), createAggregationQuerySegment(ruleId, aggregation));
        }
        //Create group query and prepare dsd
        String query = createGroupQuery(groups, groupsKey, dsd, sourceData);
        updateDsd(dsd, groups, groupsKey);
        //Return correspondent "query" step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setData(query);
        step.setDsd(dsd);
        if (type==StepType.query) {
            step.setParams(((QueryStep) source).getParams());
            step.setTypes(((QueryStep) source).getTypes());
        }
        return step;
    }


    private void addLanguageColumnsAggregations (Collection<Aggregation> aggregations, GroupParams params, DSDDataset sourceDSD) {
        Language[] languages = DatabaseStandards.getLanguageInfo();
        if (languages!=null && languages.length>0) {
            Set<String> paramsColumns = new HashSet<>(Arrays.asList(params.getBy()));
            for (Aggregation aggregation : params.getAggregations())
                paramsColumns.addAll(Arrays.asList(aggregation.getColumns()));

            for (DSDColumn column : sourceDSD.getColumns())
                if ((column.getDataType() == DataType.code || column.getDataType() == DataType.customCode) && paramsColumns.contains(column.getId()))
                    for (Language l : languages) {
                        String columnName = column.getId() + '_' + l.getCode();
                        if (!paramsColumns.contains(columnName)) {
                            Aggregation a = new Aggregation();
                            a.setRule("FIRST");
                            a.setColumns(new String[]{columnName});
                            aggregations.add(a);
                        }
                    }
        }
    }


    private String createAggregationQuerySegment(String ruleId, Aggregation aggregation) {
        //define aggregation columns
        String[] columns;
        if (ruleId!=null) { //add rule ID as the first parameter for custom aggregation functions
            columns = new String[aggregation.getColumns().length+1];
            int i=1;
            for (String column : aggregation.getColumns())
                columns[i++] = column;
            columns[0] = '\''+ruleId+'\'';
        } else
            columns = aggregation.getColumns();
        //Create query segment
        StringBuilder query = new StringBuilder(aggregation.getRule());
        if (query.indexOf("(")<0) {//if the rule contains only the name
            query.append('(');
            for (String column : columns)
                query.append(column).append(',');
            query.setCharAt(query.length()-1,')');
        } //else the query segment is the one specified into the rule fileld
        //Return query segment
        return query.toString();
    }



    private void updateDsd (DSDDataset dsd, Map<String,String> groups, Set<String> groupKeys) {
        //Select columns
        Collection<DSDColumn> columns = new LinkedList<>();
        for (DSDColumn column : dsd.getColumns()) {
            if (groupKeys.contains(column.getId())) {
                column.setKey(true);
                columns.add(column);
            } else if (groups.containsKey(column.getId())){
                column.setKey(false);
                columns.add(column);
            }
        }
        dsd.setColumns(columns);
        //Support labels into DSD
        Language[] languages = DatabaseStandards.getLanguageInfo();
        if (languages!=null && languages.length>0)
            dsd.extend(languages);
    }

    private String createGroupQuery(Map<String,String> groups, Set<String> groupKeys, DSDDataset dsd, String source) throws Exception {
        //Prepare select section
        StringBuilder query = new StringBuilder("SELECT ");
        for (DSDColumn column : dsd.getColumns())
            if (groupKeys.contains(column.getId()))
                query.append(column.getId()).append(',');
            else if (groups.containsKey(column.getId()))
                query.append(groups.get(column.getId())).append(" AS ").append(column.getId()).append(',');
        //Finish query build
        query.setLength(query.length() - 1);
        query.append(" FROM ").append(source);
        if (groupKeys.size()>0) {
            query.append(" GROUP BY ");
            for (String gk : groupKeys)
                query.append(gk).append(',');
            query.setLength(query.length() - 1);
        }
        return formatVariables(query.toString());
    }


}
