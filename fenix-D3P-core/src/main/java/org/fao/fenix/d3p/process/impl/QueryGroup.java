package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.*;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.dto.Aggregation;
import org.fao.fenix.d3p.process.dto.GroupParams;
import org.fao.fenix.d3p.process.impl.group.RulesFactory;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Column;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.dto.dataset.Type;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

@ProcessName("group")
public class QueryGroup extends org.fao.fenix.d3p.process.StatefulProcess<GroupParams> {

    private @Inject RulesFactory rulesFactory;
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;

    private String pid;

    @Override
    public void dispose(Connection connection) throws Exception {
        rulesFactory.delRule(pid);
    }

    @Override
    public Step process(Connection connection, GroupParams params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 ? (TableStep)sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("query filter can be applied only on a table or an other select query");
        String tableName = source!=null ? (String)source.getData() : null;
        DSDDataset dsd = source!=null ? source.getDsd() : null;

        Map<String, String> groups = new HashMap<>();
        String pid = null;//TODO
        for (Aggregation aggregation : params.getAggregations())
            groups.put(aggregation.getCid(), rulesFactory.setRule(aggregation.getRule(), pid, aggregation.getParameters(), dsd, aggregation.getColumns()));

        return null;
/*




        if (tableName!=null && dsd!=null) {
            //Normalize table name
            tableName = type==StepType.table ? getCacheStorage().getTableName(tableName) : '('+tableName+')';
            //Create query
            Collection<Object> queryParameters = new LinkedList<>();
            String query = createCacheFilterQuery(null, params, new Table(tableName, dsd), queryParameters);
            //Generate and return query step
            QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
            step.setDsd(filter(dsd, params));
            step.setData(query);
            step.setParams(queryParameters.toArray());
            return step;
        } else
            throw new Exception ("Source step for data filtering is unavailable or incomplete.");    */
    }

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
                else if (column.getKey())
                    throw new UnsupportedOperationException("Cannot remove key columns from selection");
            dsd.setColumns(columns);
        } else
            dsd.setColumns(source.getColumns());
        return dsd;
    }

    private String createCacheFilterQuery(Order ordering, DataFilter filter, Table table, Collection<Object> params) throws Exception {
        Map<String, Column> columnsByName = table.getColumnsByName();

        StringBuilder query = new StringBuilder("SELECT ");

        //Add select columns
        Collection<String> selectColumns = filter!=null ? filter.getColumns() : null;
        if (selectColumns!=null && selectColumns.size()>0) {
            selectColumns.retainAll(columnsByName.keySet()); //use only existing columns
            for (String name : selectColumns)
                query.append(name).append(',');
            query.setLength(query.length()-1);
        } else {
            for (Column column : table.getColumns())
                if (column.getType()== Type.array)
                    query.append("ARRAY_GET (").append(column.getName()).append(",1), ");
                else
                    query.append(column.getName()).append(", ");
            query.setLength(query.length()-2);
        }

        //Add source table
        query.append(" FROM ").append(table.getTableName());
        //Add where condition
        StandardFilter rowsFilter = filter!=null ? filter.getRows() : null;
        if (rowsFilter!=null && rowsFilter.size()>0) {
            query.append(" WHERE 1=1");
            for (Map.Entry<String, FieldFilter> conditionEntry : rowsFilter.entrySet()) {
                String fieldName = conditionEntry.getKey();
                Column column = columnsByName.get(fieldName);
                FieldFilter fieldFilter = conditionEntry.getValue();

                if (column==null)
                    throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);

                Type columnType = column.getType();
                if (fieldFilter!=null) {
                    switch (fieldFilter.getFilterType()) {
                        case enumeration:
                            if (columnType!=Type.string)
                                throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                            query.append(" AND ").append(fieldName).append(" IN (");
                            for (String value : fieldFilter.enumeration) {
                                query.append("?,");
                                params.add(value);
                            }
                            query.setCharAt(query.length() - 1, ')');
                            break;
                        case time:
                            if (columnType!=Type.integer)
                                throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                            query.append(" AND (");
                            for (TimeFilter timeFilter : fieldFilter.time) {
                                if (timeFilter.from!=null) {
                                    query.append(fieldName).append(" >= ?");
                                    params.add(timeFilter.getFrom(column.getPrecision()));
                                }
                                if (timeFilter.to!=null) {
                                    if (timeFilter.from!=null)
                                        query.append(" AND ");
                                    query.append(fieldName).append(" <= ?");
                                    params.add(timeFilter.getTo(column.getPrecision()));
                                }
                                query.append(" OR ");
                            }
                            query.setLength(query.length()-4);
                            query.append(')');
                            break;
                        case code:
                            query.append(" AND ");
                            if (columnType==Type.string) {
                                query.append(fieldName).append(" IN (");
                                for (CodesFilter codesFilter : fieldFilter.codes)
                                    for (String code : codesFilter.codes) {
                                        query.append("?,");
                                        params.add(code);
                                    }
                                query.setCharAt(query.length()-1, ')');
                            } else if (columnType==Type.array) {
                                query.append('(');
                                for (CodesFilter codesFilter : fieldFilter.codes)
                                    for (String code : codesFilter.codes) {
                                        query.append("ARRAY_CONTAINS (").append(fieldName).append(", ?) OR ");
                                        params.add(code);
                                    }
                                query.setLength(query.length()-4);
                                query.append(')');
                            } else
                                throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                    }

                }
            }
        }

        //Add ordering
        if (ordering!=null)
            query.append(ordering.toH2SQL(columnsByName.keySet().toArray(new String[columnsByName.size()])));

        //Return query
        return query.toString();
    }


}
