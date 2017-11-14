package org.fao.fenix.d3p.process;


import org.fao.fenix.commons.find.dto.filter.*;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.OjCodeList;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.JSONUtils;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.dto.VariableStorage;
import org.fao.fenix.d3s.cache.dto.dataset.Column;
import org.fao.fenix.d3s.cache.dto.dataset.Table;

import javax.decorator.Decorator;
import javax.decorator.Delegate;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.MalformedInputException;
import java.util.*;

@Dependent
public abstract class Process<T> {
    @Inject UIDUtils uidUtils;
    @Inject JSONUtils jsonUtils;
    @Inject VariableStorage variableStorage;

    /**
     * Get the expected external parameters Java type. This method is used to parse JSON payload.
     *
     * @return Parameters Java type
     */
    public Type getParametersType() {
        Type genericSuperClass = this.getClass().getGenericSuperclass();
        return genericSuperClass != null && genericSuperClass instanceof ParameterizedType ? ((ParameterizedType) genericSuperClass).getActualTypeArguments()[0] : null;
    }

    /**
     * Execute the process. It's a synchronous activity.
     *
     * @param sourceStep previous step
     * @param params     Current process external parameters.
     * @return
     */
    public abstract Step process(T params, Step[] sourceStep) throws Exception;



    //Variables management
    public Object getVariable(String name) {
        return variableStorage.getVariable(name);
    }
    public Object setChannelVariable(String name, Object[] value) {
        return variableStorage.setChannelVariable(name, value);
    }
    public Object setGlobalVariable(String name, Object[] value) {
        return variableStorage.setGlobalVariable(name,value);
    }

    public String formatVariables(String text) throws Exception {
        StringBuilder buffer = new StringBuilder(text);
        for (int from=buffer.indexOf("<<"); from>=0 && from<buffer.length(); from=buffer.indexOf("<<",from)) {
            int to = buffer.indexOf(">>",from);
            if (to>0) {
                String value = getVariableString(buffer.substring(from+2,to));
                if (value==null)
                    throw new BadRequestException("Variable not found: "+buffer.substring(from+2,to));
                buffer.replace(from,to+2,value);
                from = 0;
            } else
                from = -1;
        }
        return buffer.toString();
    }
    public String getVariableString(String name) throws Exception {
        return JSONUtils.toJSON(getVariable(name)).replace('[','(').replace(']',')').replace('"','\'');
    }

    //UTILS
    protected String getRandomTmpTableName() {
        return "TMP_" + uidUtils.newId();
    }

    protected String createCacheFilterQuery(Order ordering, DataFilter filter, Table table, Collection<Object> params, Collection<Integer> types, Collection<DSDColumn> dsdColumns, Collection<Step> otherDatasets) throws Exception {
        Map<String, Column> columnsByName = table.getColumnsByName();

        StringBuilder query = new StringBuilder("SELECT ");

        //Add select columns
        Collection<String> selectColumns = filter != null ? filter.getColumns() : null;
        if (selectColumns != null && selectColumns.size() > 0) {
            selectColumns.retainAll(columnsByName.keySet()); //use only existing columns
            for (String name : selectColumns)
                query.append(name).append(',');
            query.setLength(query.length() - 1);
        } else {
            for (Column column : table.getColumns())
                if (column.getType() == org.fao.fenix.d3s.cache.dto.dataset.Type.array)
                    query.append("ARRAY_GET (").append(column.getName()).append(",1), ");
                else
                    query.append(column.getName()).append(", ");
            query.setLength(query.length() - 2);
        }

        //Add source table
        query.append(" FROM ").append(table.getTableName());

        //Add where condition
        StandardFilter rowsFilter = filter != null ? filter.getRows() : null;
        appendWhereCondition(query, rowsFilter, table, params, types, dsdColumns, otherDatasets);

        //Add ordering
        if (ordering != null)
            query.append(ordering.toH2SQL(columnsByName.keySet().toArray(new String[columnsByName.size()])));

        //Return query with variables
        return formatVariables(query.toString());
    }

    protected String createCacheDeleteQuery(StandardFilter rowsFilter, Table table, Collection<Object> params, Collection<Integer> types, Collection<DSDColumn> dsdColumns) throws Exception {
        StringBuilder query = new StringBuilder("DELETE FROM ").append(table.getTableName());
        //Add where condition
        appendWhereCondition(query, rowsFilter, table, params, types, dsdColumns, null);
        //Return query
        return query.toString();
    }

    protected String getTmpId(String uid, String version) {
        return uid != null ? (version != null ? uid + "@@@" + version : uid) : null;
    }

    protected void appendWhereCondition(StringBuilder query, StandardFilter rowsFilter, Table table, Collection<Object> params, Collection<Integer> types, Collection<DSDColumn> dsdColumns, Collection<Step> otherSteps) throws Exception {
        //TODO support types
        Map<String, Column> columnsByName = table.getColumnsByName();
        Map<String, String> codeLists = new HashMap<>();
        for (DSDColumn column : dsdColumns)
            if (column.getDataType() == DataType.code) {
                OjCodeList codeList = column.getDomain().getCodes().iterator().next();
                codeLists.put(column.getId(), getTmpId(codeList.getIdCodeList(), codeList.getVersion()));
            }

        //Add where condition
        StringBuilder whereCondition = new StringBuilder();
        if (rowsFilter != null) {
            for (Map.Entry<String, FieldFilter> conditionEntry : rowsFilter.entrySet()) {
                String fieldName = conditionEntry.getKey();
                boolean exclude = fieldName.startsWith("!");
                if (exclude)
                    fieldName = fieldName.substring(1);

                Column column = columnsByName.get(fieldName);
                FieldFilter fieldFilter = conditionEntry.getValue();

                if (column == null)
                    throw new Exception("Wrong table structure for filter:" + table.getTableName() + '.' + fieldName);

                org.fao.fenix.d3s.cache.dto.dataset.Type columnType = column.getType();
                if (fieldFilter != null) {
                    switch (fieldFilter.retrieveFilterType()) {
                        case enumeration:
                            if (columnType != org.fao.fenix.d3s.cache.dto.dataset.Type.string)
                                throw new Exception("Wrong table structure for filter:" + table.getTableName() + '.' + fieldName);
                            whereCondition.append(" AND ").append(fieldName).append(exclude ? " NOT IN (" : " IN (");
                            for (String value : fieldFilter.enumeration) {
                                whereCondition.append("?,");
                                params.add(value);
                            }
                            whereCondition.setCharAt(whereCondition.length() - 1, ')');
                            break;
                        case table:
                            validate(fieldFilter, dsdColumns, otherSteps);
                            for (TableFilter filter : fieldFilter.tables) {
                                whereCondition.append(" AND ").append(fieldName).append(exclude ? " NOT IN (" : " IN (");
                                Step tmpStep = null;
                                for (Step step : otherSteps)
                                    if (step.getRid().getId().equals(filter.getUid()))
                                        tmpStep = step;

                                if (tmpStep == null)
                                    throw new Exception("Something wrong on steps");

                                switch (tmpStep.getType()) {

                                    case query:
                                        handleDynamicQueryStep(params, tmpStep, dsdColumns, whereCondition, filter, otherSteps);
                                        break;

                                    case iterator:

                                        IteratorStep iteratorStep = (IteratorStep) tmpStep;
                                        Iterator<Object[]> it = iteratorStep.getData();
                                        while (it.hasNext()) {
                                            whereCondition.append("?,");
                                            params.add(it.next());
                                        }
                                        whereCondition.append(" )");
                                        break;

                                    case table:

                                        TableStep step = (TableStep) tmpStep;
                                        String querySelect = "SELECT " + fieldName + " FROM " + step.getData();
                                        String ridToAppend = (step.getRid() != null && step.getRid().getId() != null) ? "AS " + step.getRid().getId() : "";
                                        whereCondition.append(querySelect).append(ridToAppend).append(" )");
                                        break;
                                }
                                whereCondition.append(" OR ");
                            }
                            whereCondition.setLength(whereCondition.length() - 4);

                            break;
                        case time:
                            if (columnType!=org.fao.fenix.d3s.cache.dto.dataset.Type.integer)
                                throw new Exception("Wrong table structure for filter:" + table.getTableName() + '.' + fieldName);
                            whereCondition.append(" AND (");
                            for (TimeFilter timeFilter : fieldFilter.time) {
                                if (timeFilter.from != null) {
                                    whereCondition.append(fieldName).append(exclude ? " < ?" : " >= ?");
                                    params.add(timeFilter.getFrom(column.getPrecision()));
                                }
                                if (timeFilter.to != null) {
                                    if (timeFilter.from != null)
                                        whereCondition.append(" AND ");
                                    whereCondition.append(fieldName).append(exclude ? " > ?" : " <= ?");
                                    params.add(timeFilter.getTo(column.getPrecision()));
                                }
                                whereCondition.append(" OR ");
                            }
                            whereCondition.setLength(whereCondition.length() - 4);
                            whereCondition.append(')');
                            break;
                        case number:
                            if (columnType!=org.fao.fenix.d3s.cache.dto.dataset.Type.integer && columnType!=org.fao.fenix.d3s.cache.dto.dataset.Type.real)
                                throw new Exception("Wrong table structure for filter:" + table.getTableName() + '.' + fieldName);
                            whereCondition.append(" AND (");
                            for (NumberFilter numberFilter : fieldFilter.number) {
                                if (numberFilter.from != null) {
                                    whereCondition.append(fieldName).append(exclude ? " < ?" : " >= ?");
                                    params.add(columnType==org.fao.fenix.d3s.cache.dto.dataset.Type.integer ? numberFilter.from.longValue() : numberFilter.from);
                                }
                                if (numberFilter.to != null) {
                                    if (numberFilter.from != null)
                                        whereCondition.append(" AND ");
                                    whereCondition.append(fieldName).append(exclude ? " > ?" : " <= ?");
                                    params.add(columnType==org.fao.fenix.d3s.cache.dto.dataset.Type.integer ? numberFilter.to.longValue() : numberFilter.to);
                                }
                                whereCondition.append(" OR ");
                            }
                            whereCondition.setLength(whereCondition.length() - 4);
                            whereCondition.append(')');
                            break;
                        case code:
                            String codeList = codeLists.get(fieldName);
                            whereCondition.append(" AND ");
                            if (columnType == org.fao.fenix.d3s.cache.dto.dataset.Type.string) {
                                whereCondition.append(fieldName).append(exclude ? " NOT IN (" : " IN (");
                                for (CodesFilter codesFilter : fieldFilter.codes) {
                                    String filterCodeList = getTmpId(codesFilter.uid, codesFilter.version);
                                    if (codeList == null || filterCodeList == null || !codeList.equals(filterCodeList))
                                        throw new Exception("Wrong table structure for filter:" + table.getTableName() + '.' + fieldName);
                                    for (String code : codesFilter.codes) {
                                        whereCondition.append("?,");
                                        params.add(code);
                                    }
                                }
                                whereCondition.setCharAt(whereCondition.length() - 1, ')');
                            } else if (columnType == org.fao.fenix.d3s.cache.dto.dataset.Type.array) {
                                whereCondition.append(exclude ? " NOT (" : "(");
                                for (CodesFilter codesFilter : fieldFilter.codes) {
                                    String filterCodeList = getTmpId(codesFilter.uid, codesFilter.version);
                                    if (codeList == null || filterCodeList == null || !codeList.equals(filterCodeList))
                                        throw new Exception("Wrong table structure for filter:" + table.getTableName() + '.' + fieldName);
                                    for (String code : codesFilter.codes) {
                                        whereCondition.append("ARRAY_CONTAINS (").append(fieldName).append(", ?) OR ");
                                        params.add(code);
                                    }
                                }
                                whereCondition.setLength(whereCondition.length() - 4);
                                whereCondition.append(')');
                            } else
                                throw new Exception("Wrong table structure for filter:" + table.getTableName() + '.' + fieldName);
                            break;
                        case bool:
                            if (columnType != org.fao.fenix.d3s.cache.dto.dataset.Type.bool)
                                throw new Exception("Wrong table structure for filter:" + table.getTableName() + '.' + fieldName);
                            whereCondition.append(" AND ").append(fieldName).append(exclude ? " != ? " : " = ? ");
                            params.add(fieldFilter.bool);
                            break;
                        case var:
                            //TODO verify column data type and values type cmpatibility
                            Object value = getVariable(fieldFilter.variable);
                            if (value instanceof Object[]) {
                                if (((Object[])value).length>0) {
                                    whereCondition.append(" AND ").append(fieldName).append(exclude ? " NOT IN (" : " IN (");
                                    for (Object v : (Object[]) value) {
                                        whereCondition.append("?,");
                                        params.add(v);
                                    }
                                    whereCondition.setCharAt(whereCondition.length() - 1, ')');
                                } else
                                    whereCondition.append(" AND 1<>1");
                            } else {
                                whereCondition.append(" AND ").append(fieldName).append(exclude ? " != ? " : " = ? ");
                                params.add(value);
                            }
                    }

                }
            }
        }
        if (whereCondition.length() > 0)
            query.append(" WHERE ").append(whereCondition.substring(5));
    }


    private void validate(FieldFilter filters, Collection<DSDColumn> dsdColumns, Collection<Step> otherSteps) throws Exception {

       /*
            1) check parameters of table filter
            2) check dataset exists into the sid parameter
            3) check column exists into the dataset
            4) check column compatibility between source column and dynamic column
        */
        if (otherSteps == null)
            throw new Exception("Wrong table structure for filter parameters: sid parameters miss");

        for (TableFilter filter : filters.tables) {
            DSDDataset associatedDataset = null;
            DSDColumn associatedColumn = null;
            DSDColumn sourceColumn = null;

            String uid = (filter.getUid() != null) ? filter.getUid() : null;
            String columnID = (filter.getColumn() != null) ? filter.getColumn() : null;

            if (uid == null)
                throw new Exception("Wrong table structure for filter parameters: uid parameter misses");
            if (columnID == null)
                throw new Exception("Wrong table structure for filter parameters: column parameter misses");


            for (Step step : otherSteps) {
                if (step.getRid().getId().equals(uid))
                    associatedDataset = step.getCurrentDsd();
            }
            if (associatedDataset == null)
                throw new Exception("Wrong table structure for filter parameters: sid is not compatible with the dataset");
            for (DSDColumn column : associatedDataset.getColumns())
                if (column.getId().equals(columnID))
                    associatedColumn = column;
            if (associatedColumn == null)
                throw new Exception("Wrong table structure for filter parameters: this column " + columnID + " does not exists into the parameters");
            for (DSDColumn column : dsdColumns)
                if (column.getId().equals(columnID))
                    sourceColumn = column;
            if (sourceColumn == null)
                throw new Exception("Wrong table structure for filter parameters: this column " + columnID + " does not exists into the source dataset");

            if (sourceColumn.getDataType() != associatedColumn.getDataType())
                throw new Exception("Wrong configuration: different datatype between columns");
            if (sourceColumn.getDataType().equals(DataType.code) && sourceColumn.getDomain().getCodes().iterator().next().getIdCodeList() != associatedColumn.getDomain().getCodes().iterator().next().getIdCodeList())
                throw new Exception("Wrong configuration: domain is different between two columns");
        }
    }


    // handle dynamic query step
    private void handleDynamicQueryStep(Collection<Object> params, Step tmpStep, Collection<DSDColumn> dsdColumns, StringBuilder whereCondition, TableFilter filter, Collection<Step> otherSteps) throws Exception {

        // add parameters
        params.addAll(Arrays.asList(((QueryStep) (tmpStep)).getParams()));
        DSDDataset otherDataset = null;
        Iterator<Step> itSteps = otherSteps.iterator();
        boolean found = false;
        Step tmp = null;
        while (itSteps.hasNext() && !found) {
            tmp = itSteps.next();
            if (tmp.getRid().getId().equals(filter.getUid())) {
                found = true;
                otherDataset = tmp.getDsd();
            }
        }

        if (tmp == null)
            throw new Exception("Something went wrong");
        // query to select data
        String queryString = "( " + tmp.getData().toString() + " ) AS " + filter.getUid();
        DataFilter dataFilter = new DataFilter();
        dataFilter.addColumn(filter.getColumn());
        whereCondition.append(createCacheFilterQuery(null, dataFilter, new Table(queryString, otherDataset), params, null, dsdColumns, null)).append(" )");
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