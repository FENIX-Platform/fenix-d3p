package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.dto.Aggregation;
import org.fao.fenix.d3p.process.dto.GroupParams;
import org.fao.fenix.d3p.process.impl.group.RulesFactory;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.*;

@ProcessName("group")
public class Group extends org.fao.fenix.d3p.process.StatefulProcess<GroupParams> {

    private @Inject RulesFactory rulesFactory;
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;
    private @Inject UIDUtils uidUtils;

    private String pid;

    @Override
    public void dispose(Connection connection) throws Exception {
        rulesFactory.delRule(pid);
    }

    @Override
    public Step process(Connection connection, GroupParams params, Step... sourceStep) throws Exception {
        pid = uidUtils.getId();
        //Retrieve source informations
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("query filter can be applied only on a table or an other select query");
        String sourceData = (String)source.getData();
        sourceData = type==StepType.table ? sourceData : '('+sourceData+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();
        Set<String> groupsKey = new HashSet<>(Arrays.asList(params.getBy()));
        //Append label aggregations if needed
        Collection<Aggregation> aggregations = new LinkedList<>(Arrays.asList(params.getAggregations()));
        Language[] languages = DatabaseStandards.getLanguageInfo();
        if (languages!=null && languages.length>0)
            for (DSDColumn column : dsd.getColumns())
                if ((column.getDataType()== DataType.code || column.getDataType()==DataType.customCode) && groupsKey.contains(column.getId()))
                    for (Language l : languages) {
                        Aggregation a = new Aggregation();
                        a.setRule("FIRST");
                        a.setColumns(new String[]{column.getId() + '_' + l.getCode()});
                        aggregations.add(a);
                    }
        //Define groups rule
        Map<String, String> groups = new HashMap<>();
        for (Aggregation aggregation : aggregations) {
            String ruleId = rulesFactory.setRule(aggregation.getRule(), pid, aggregation.getParameters(), dsd, aggregation.getColumns());
            groups.put(aggregation.getCid(), createAggregationQuerySegment(ruleId, aggregation));
        }
        //Create group query and prepare dsd
        String query = createGroupQuery(groups, groupsKey, dsd, sourceData);
        dsd = filter(dsd, groups, groupsKey);
        //Return correspondent "query" step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(query);
        if (type==StepType.query) {
            step.setParams(((QueryStep) source).getParams());
            step.setTypes(((QueryStep) source).getTypes());
        }
        step.setRid(getRandomTmpTableName());
        return step;
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



    private DSDDataset filter (DSDDataset source, Map<String,String> groups, Set<String> groupKeys) {
        DSDDataset dsd = new DSDDataset();
        dsd.setAggregationRules(source.getAggregationRules());
        dsd.setContextSystem("D3P");
        //Select columns
        Collection<DSDColumn> columns = new LinkedList<>();
        for (DSDColumn column : source.getColumns()) {
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
            dsd.extend(false, languages);
        //Return dsd
        return dsd;
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
        return query.toString();
    }


}
