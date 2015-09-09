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
            tableName = type==StepType.table ? getCacheStorage().getTableName(tableName) : '('+tableName+')';
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

    private String createCacheFilterQuery(Order ordering, DataFilter filter, Table table, Collection<Object> params, Collection<DSDColumn> dsdColumns) throws Exception {
        Map<String, Column> columnsByName = table.getColumnsByName();
        Map<String, String> codeLists = new HashMap<>();
        for (DSDColumn column : dsdColumns)
            if (column.getDataType()== DataType.code) {
                OjCodeList codeList = column.getDomain().getCodes().iterator().next();
                codeLists.put(column.getId(), getId(codeList.getIdCodeList(), codeList.getVersion()));
            }

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

            //Order filter on key columns
            final ArrayList<String> keyColumns = new ArrayList<>();
            for (Column column : table.getColumns())
                if (column.isKey())
                    keyColumns.add(column.getName());

            LinkedHashMap<String, FieldFilter> rowsFilterOrdered = null;
            if (keyColumns.size()>0) {
                List<String> orderedColumns = new LinkedList<>(rowsFilter.keySet());
                Collections.sort(orderedColumns, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        int index_o1 = keyColumns.indexOf(o1);
                        index_o1 = index_o1 < 0 ? Integer.MAX_VALUE : index_o1;
                        int index_o2 = keyColumns.indexOf(o2);
                        index_o2 = index_o2 < 0 ? Integer.MAX_VALUE : index_o2;
                        if (index_o1 == index_o2)
                            return o1.compareTo(o2);
                        else if (index_o1 < index_o2)
                            return -1;
                        else
                            return 1;
                    }
                });

                rowsFilterOrdered = new LinkedHashMap<>();
                for (String column : orderedColumns)
                    rowsFilterOrdered.put(column, rowsFilter.get(column));
            } else {
                rowsFilterOrdered = new LinkedHashMap<>(rowsFilter);
            }

            //Build where condition
            query.append(" WHERE 1=1");
            for (Map.Entry<String, FieldFilter> conditionEntry : rowsFilterOrdered.entrySet()) {
                String fieldName = conditionEntry.getKey();
                Column column = columnsByName.get(fieldName);
                FieldFilter fieldFilter = conditionEntry.getValue();

                if (column==null)
                    throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);

                Type columnType = column.getType();
                if (fieldFilter!=null) {
                    switch (fieldFilter.retrieveFilterType()) {
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
                            String codeList = codeLists.get(fieldName);
                            query.append(" AND ");
                            if (columnType==Type.string) {
                                query.append(fieldName).append(" IN (");
                                for (CodesFilter codesFilter : fieldFilter.codes) {
                                    String filterCodeList = getId(codesFilter.uid, codesFilter.version);
                                    if (codeList==null || filterCodeList==null || !codeList.equals(filterCodeList))
                                        throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                                    for (String code : codesFilter.codes) {
                                        query.append("?,");
                                        params.add(code);
                                    }
                                }
                                query.setCharAt(query.length()-1, ')');
                            } else if (columnType==Type.array) {
                                query.append('(');
                                for (CodesFilter codesFilter : fieldFilter.codes) {
                                    String filterCodeList = getId(codesFilter.uid, codesFilter.version);
                                    if (codeList==null || filterCodeList==null || !codeList.equals(filterCodeList))
                                        throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                                    for (String code : codesFilter.codes) {
                                        query.append("ARRAY_CONTAINS (").append(fieldName).append(", ?) OR ");
                                        params.add(code);
                                    }
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

    private String getId(String uid, String version) {
        return uid!=null ? (version!=null ? uid + "@@@" + version : uid) : null;
    }

}


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
