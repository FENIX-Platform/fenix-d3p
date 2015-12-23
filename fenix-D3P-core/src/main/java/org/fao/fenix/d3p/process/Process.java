package org.fao.fenix.d3p.process;



import org.fao.fenix.commons.find.dto.filter.*;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.OjCodeList;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3s.cache.dto.dataset.Column;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.*;

public abstract class Process <T> {
    @Inject UIDUtils uidUtils;

    private DatasetStorage cacheStorage;
    public final void init (DatasetStorage cacheStorage) {
        this.cacheStorage = cacheStorage;
    }

    /**
     * Get the expected external parameters Java type. This method is used to parse JSON payload.
     * @return Parameters Java type
     */
    public Type getParametersType() {
        Type genericSuperClass = this.getClass().getGenericSuperclass();
        return genericSuperClass!=null && genericSuperClass instanceof ParameterizedType ? ((ParameterizedType)genericSuperClass).getActualTypeArguments()[0] : null;
    }

    /**
     * Execute the process. It's a synchronous activity.
     * @param sourceStep previous step
     * @param params Current process external parameters.
     * @return
     */
    public abstract Step process(Connection connection, T params, Step ... sourceStep) throws Exception;


    //UTILS
    protected String getRandomTmpTableName() {
        return "TMP_"+uidUtils.getId();
    }
    protected DatasetStorage getCacheStorage() {
        return cacheStorage;
    }


    protected String createCacheFilterQuery(Order ordering, DataFilter filter, Table table, Collection<Object> params, Collection<DSDColumn> dsdColumns) throws Exception {

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
                if (column.getType()== org.fao.fenix.d3s.cache.dto.dataset.Type.array)
                    query.append("ARRAY_GET (").append(column.getName()).append(",1), ");
                else
                    query.append(column.getName()).append(", ");
            query.setLength(query.length()-2);
        }

        //Add source table
        query.append(" FROM ").append(table.getTableName());

        //Add where condition
        StandardFilter rowsFilter = filter!=null ? filter.getRows() : null;
        appendWhereCondition(query,rowsFilter,table,params,dsdColumns);

        //Add ordering
        if (ordering!=null)
            query.append(ordering.toH2SQL(columnsByName.keySet().toArray(new String[columnsByName.size()])));

        //Return query
        return query.toString();
    }

    protected String createCacheDeleteQuery(StandardFilter rowsFilter, Table table, Collection<Object> params, Collection<DSDColumn> dsdColumns) throws Exception {
        StringBuilder query = new StringBuilder("DELETE FROM ").append(table.getTableName());
        //Add where condition
        appendWhereCondition(query,rowsFilter,table,params,dsdColumns);
        //Return query
        return query.toString();
    }

    protected String getTmpId(String uid, String version) {
        return uid!=null ? (version!=null ? uid + "@@@" + version : uid) : null;
    }

    protected void appendWhereCondition (StringBuilder query, StandardFilter rowsFilter, Table table, Collection<Object> params, Collection<DSDColumn> dsdColumns) throws Exception {
        Map<String, Column> columnsByName = table.getColumnsByName();
        Map<String, String> codeLists = new HashMap<>();
        for (DSDColumn column : dsdColumns)
            if (column.getDataType()== DataType.code) {
                OjCodeList codeList = column.getDomain().getCodes().iterator().next();
                codeLists.put(column.getId(), getTmpId(codeList.getIdCodeList(), codeList.getVersion()));
            }

        //Add where condition
        StringBuilder whereCondition = new StringBuilder();
        if (rowsFilter!=null)
            for (Map.Entry<String, FieldFilter> conditionEntry : rowsFilter.entrySet()) {
                String fieldName = conditionEntry.getKey();
                Column column = columnsByName.get(fieldName);
                FieldFilter fieldFilter = conditionEntry.getValue();

                if (column==null)
                    throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);

                org.fao.fenix.d3s.cache.dto.dataset.Type columnType = column.getType();
                if (fieldFilter!=null) {
                    switch (fieldFilter.retrieveFilterType()) {
                        case enumeration:
                            if (columnType!= org.fao.fenix.d3s.cache.dto.dataset.Type.string)
                                throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                            whereCondition.append(" AND ").append(fieldName).append(" IN (");
                            for (String value : fieldFilter.enumeration) {
                                whereCondition.append("?,");
                                params.add(value);
                            }
                            whereCondition.setCharAt(whereCondition.length() - 1, ')');
                            break;
                        case time:
                            if (columnType!= org.fao.fenix.d3s.cache.dto.dataset.Type.integer)
                                throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                            whereCondition.append(" AND (");
                            for (TimeFilter timeFilter : fieldFilter.time) {
                                if (timeFilter.from!=null) {
                                    whereCondition.append(fieldName).append(" >= ?");
                                    params.add(timeFilter.getFrom(column.getPrecision()));
                                }
                                if (timeFilter.to!=null) {
                                    if (timeFilter.from!=null)
                                        whereCondition.append(" AND ");
                                    whereCondition.append(fieldName).append(" <= ?");
                                    params.add(timeFilter.getTo(column.getPrecision()));
                                }
                                whereCondition.append(" OR ");
                            }
                            whereCondition.setLength(whereCondition.length()-4);
                            whereCondition.append(')');
                            break;
                        case code:
                            String codeList = codeLists.get(fieldName);
                            whereCondition.append(" AND ");
                            if (columnType== org.fao.fenix.d3s.cache.dto.dataset.Type.string) {
                                whereCondition.append(fieldName).append(" IN (");
                                for (CodesFilter codesFilter : fieldFilter.codes) {
                                    String filterCodeList = getTmpId(codesFilter.uid, codesFilter.version);
                                    if (codeList==null || filterCodeList==null || !codeList.equals(filterCodeList))
                                        throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                                    for (String code : codesFilter.codes) {
                                        whereCondition.append("?,");
                                        params.add(code);
                                    }
                                }
                                whereCondition.setCharAt(whereCondition.length()-1, ')');
                            } else if (columnType== org.fao.fenix.d3s.cache.dto.dataset.Type.array) {
                                whereCondition.append('(');
                                for (CodesFilter codesFilter : fieldFilter.codes) {
                                    String filterCodeList = getTmpId(codesFilter.uid, codesFilter.version);
                                    if (codeList==null || filterCodeList==null || !codeList.equals(filterCodeList))
                                        throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                                    for (String code : codesFilter.codes) {
                                        whereCondition.append("ARRAY_CONTAINS (").append(fieldName).append(", ?) OR ");
                                        params.add(code);
                                    }
                                }
                                whereCondition.setLength(whereCondition.length()-4);
                                whereCondition.append(')');
                            } else
                                throw new Exception("Wrong table structure for filter:"+table.getTableName()+'.'+fieldName);
                    }

                }
            }
        if (whereCondition.length()>0)
            query.append(" WHERE ").append(whereCondition.substring(5));
    }

}





/*
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
*/