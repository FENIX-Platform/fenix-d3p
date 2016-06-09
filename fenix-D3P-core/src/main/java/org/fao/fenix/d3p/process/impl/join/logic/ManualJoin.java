package org.fao.fenix.d3p.process.impl.join.logic;

import org.apache.log4j.Logger;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.OjCodeList;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.JoinParameter;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.dto.JoinValueTypes;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

public class ManualJoin implements JoinLogic {
    private @Inject StepFactory stepFactory;

    private static final Logger LOGGER = Logger.getLogger(ManualJoin.class);


    @Override
    public Step process(Step[] sourceStep, DSDDataset[] dsdList, JoinParams params) throws Exception {

        //Create column key DSD
        LOGGER.debug("start join process");
        JoinParameter[][] joinParameters = params.getJoins();
        List<DSDColumn> keyColumns = new LinkedList<>();
        Set<String>[] joinColumnNames = new HashSet[joinParameters.length];
        for (int c = 0; c < joinParameters[0].length; c++) {
            boolean keyColumnFound = false;
            for (int r = 0; r < joinParameters.length; r++) {
                if (joinParameters[r][c].getType() == JoinValueTypes.id) {
                    if (!keyColumnFound) {
                        keyColumns.add(dsdList[r].findColumn((String) joinParameters[r][c].getValue()));
                        keyColumnFound = true;
                    }
                    if (joinColumnNames[r] == null)
                        joinColumnNames[r] = new HashSet<>();
                    joinColumnNames[r].add((String) joinParameters[r][c].getValue());
                }
            }
        }

        //Create column values DSD
        List<DSDColumn> valueColumns = new LinkedList<>();
        String[][] valueParameters = params.getValues();
        // if values parameters do not exist, create a structure with the length of the datasource - the key column
        if (valueParameters == null || valueParameters.length == 0)
            valueParameters = new String[joinParameters.length][];
        for (int r = 0; r < valueParameters.length; r++)
            if (valueParameters[r] != null && valueParameters[r].length > 0) {
                // // if values parameters exist, update value column with those values
                Collection<DSDColumn> datasetValueColumns = getValueColumns(dsdList[r].getColumns(), valueParameters[r]);
                Collection<String> datasetValueColumnsName = new LinkedList<>();
                for (DSDColumn datasetValueColumn : datasetValueColumns) {
                    datasetValueColumnsName.add(datasetValueColumn.getId());
                    valueColumns.add(updateId(datasetValueColumn, sourceStep[r].getRid().getId()));
                }
                valueParameters[r] = datasetValueColumnsName.toArray(new String[datasetValueColumnsName.size()]);
            } else
                //else fill it with the remaining columns of the dataset
                for (int c = 0; c < dsdList[r].getColumns().size(); c++)

                    // if it is not a key column
                    if (!joinColumnNames[r].contains((((ArrayList<DSDColumn>) dsdList[r].getColumns()).get(c)).getId())) {
                        DSDColumn column = ((ArrayList<DSDColumn>) dsdList[r].getColumns()).get(c);
                        // when a user does not specify anything, take all the columns in the dataset
                        if (valueParameters[r] == null) {
                            int sizeColumnDataset = dsdList[r].getColumns().size() - joinColumnNames[r].size();
                            valueParameters[r] = new String[sizeColumnDataset];
                        }

                        // fill value parameters
                        for (int h = 0; h < valueParameters[r].length; h++) {
                            if (valueParameters[r][h] == null) {
                                valueParameters[r][h] = column.getId();
                                break;
                            }
                        }
                        // update value column with the new id
                        valueColumns.add(updateId(column, sourceStep[r].getRid().getId()));
                    }

        //Create result dsd
        Collection<DSDColumn> joinedColumns = new LinkedList<>();
        joinedColumns.addAll(keyColumns);
        joinedColumns.addAll(valueColumns);
        createKey(keyColumns, valueColumns, dsdList, joinParameters, valueParameters);
        createSubjects(keyColumns, valueColumns, dsdList, joinParameters, valueParameters);

        DSDDataset dsd = new DSDDataset();
        dsd.setColumns(joinedColumns);
        dsd.setContextSystem("D3P");

        //Create result
        Collection<Object> queryParameters = new LinkedList<>();
        QueryStep step = (QueryStep) stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(createQuery(sourceStep, joinParameters, valueParameters, queryParameters, keyColumns));

        step.setParams(queryParameters.toArray());
        step.setTypes(null);
        return step;
    }

    /*
        Validation steps:
        1) Cells passed as parameters are not null
        2) Fixed value in the join are syntactically correct
        3) Key not empty
        4) Dimension parameters that follows the dimension of the sources
        5) Column Ids exist
        6) Check compatibility key column
        7) Check that there are not row with only fixed values in the join parameters
        8) Check that there are not columns with only fixed values in the join parameters
        9) Check that between joins and values there are not duplicated values at single dataset level
        10)Check that fixed values are the same at single column level
     */
    @Override
    public void validate(Step[] sourceStep, DSDDataset[] dsdList, JoinParams params) throws Exception {
        JoinParameter[][] joinParameters = params.getJoins();
        String[][] valueParameters = params.getValues();

        // join parameters should be not null
        if (joinParameters == null || joinParameters.length == 0)
            throw new BadRequestException("Join parameters should be not null");
        // Value paramenters should be not null and the number of rows of join parameters and value parameters are equal to number of step
        if (joinParameters.length != sourceStep.length || (valueParameters != null && valueParameters.length > 0 && valueParameters.length != sourceStep.length))
            throw new BadRequestException("Value parameters should be not null and the number of rows of join parameters and value parameters are equal to number of step");

        for (int r = 0; r < joinParameters.length; r++) {
            Set<String> ids = new HashSet<>();
            boolean valueRowId = false;

            // join parameters cycle
            for (int c = 0; c < joinParameters[r].length; c++) {

                // Check join parameters are syntactically correct and not null
                if (joinParameters[r][c] == null || joinParameters[r][c].getType() == null || joinParameters[r][c].getValue() == null)
                    throw new BadRequestException("Check join parameters are syntactically correct and not null");

                Class requiredValueClass = null;
                switch (joinParameters[r][c].getType()) {
                    case id:
                    case text:
                        requiredValueClass = String.class;
                        break;
                    case bool:
                        requiredValueClass = Boolean.class;
                        break;
                    case number:
                        requiredValueClass = Number.class;
                        break;
                }
                // Check type and value parameters are syntactically consistent
                if (!requiredValueClass.isInstance(joinParameters[r][c].getValue()))
                    throw new BadRequestException("Check type and value parameters are syntactically consistent");

                if (joinParameters[r][c].getType() == JoinValueTypes.id) {
                    // Check that there is not repetition of id
                    if (!ids.add((String) joinParameters[r][c].getValue()))
                        throw new BadRequestException("Check that there is not repetition of id");
                    valueRowId = true;

                    // Check that columns exists into the dataset
                    if (dsdList[r].findColumn((String) joinParameters[r][c].getValue()) == null)
                        throw new BadRequestException("Check that columns exists into the dataset");
                }
            }

            // value parameters cycle
            if (valueParameters != null && valueParameters.length > 0)
                for (int c = 0; c < valueParameters[r].length; c++)
                    if (valueParameters[r] != null && valueParameters[r].length > 0) {

                        // Check that cells into value parameters are not null
                        if (valueParameters[r][c] == null)
                            throw new BadRequestException("Check that cells into value parameters are not null");

                        // check that cells into value parameters are not repeated
                        if (!ids.add(valueParameters[r][c]))
                            throw new BadRequestException("Check that cells into value parameters are not repeated");

                        // check that cells into value parameters are in the dataset
                        if (dsdList[r].findColumn(valueParameters[r][c]) == null)
                            throw new BadRequestException("Check that cells into value parameters are in the dataset");
                    }
            // check that for each row into join parameter there should be specified at least one id column
            if (!valueRowId)
                throw new BadRequestException("check that for each row into join parameter there should be specified at least one id column");
        }

        for (int c = 0; c < joinParameters[0].length; c++) {
            Object value = null;
            JoinValueTypes type = null;
            DSDColumn column = null;
            boolean valueColumnId = false;

            for (int r = 0; r < joinParameters.length; r++) {
                if (joinParameters[r][c].getType() == JoinValueTypes.id) {
                    valueColumnId = true;
                    DSDColumn currentColumn = dsdList[r].findColumn((String) joinParameters[r][c].getValue());
                    if (column != null)
                        checkDomain(column, currentColumn);
                    else if (value != null)
                        checkDomain(currentColumn, value, type);
                    else
                        column = currentColumn;
                } else {
                    if (column != null)
                        checkDomain(column, joinParameters[r][c].getValue(), joinParameters[r][c].getType());
                    else if (value != null)
                        checkDomain(value, type, joinParameters[r][c].getValue(), joinParameters[r][c].getType());
                    else {
                        value = joinParameters[r][c].getValue();
                        type = joinParameters[r][c].getType();
                    }
                }
            }
            // check that for each column into join parameter there should be specified at least one id column
            if (!valueColumnId)
                throw new BadRequestException("Check that for each column into join parameter there should be specified at least one id column");
        }

    }

    private void checkDomain(DSDColumn column1, DSDColumn column2) throws Exception {
        if (column1.getDataType() != column2.getDataType())
            throw new BadRequestException("Please check that the subject of column "+ column1.getId()+" is compatible with column "+column2.getId());
        if (column1.getDataType() == DataType.code) {
            OjCodeList domain1 = column1.getDomain().getCodes().iterator().next();
            OjCodeList domain2 = column2.getDomain().getCodes().iterator().next();
            String id1 = domain1.getIdCodeList() + (domain1.getVersion() != null ? '|' + domain1.getVersion() : "");
            String id2 = domain1.getIdCodeList() + (domain2.getVersion() != null ? '|' + domain2.getVersion() : "");
            // check that the column codes are joinable
            if (!id1.equals(id2))
                throw new BadRequestException("Check that the column codes are joinable");
        }

    }

    private void checkDomain(DSDColumn column1, Object value, JoinValueTypes type) throws Exception {
        JoinValueTypes requiredType = null;
        switch (column1.getDataType()) {
            case bool:
                requiredType = JoinValueTypes.bool;
                break;
            case code:
            case customCode:
            case enumeration:
            case text:
                requiredType = JoinValueTypes.text;
                break;
            case number:
            case percentage:
            case year:
            case month:
            case date:
            case time:
                requiredType = JoinValueTypes.number;
                break;
        }
        // type specified into fixed join parameter should follow the datatype fo the column
        if (requiredType != type)
            throw new BadRequestException("Type specified into fixed join parameter should follow the datatype fo the column");
    }

    private void checkDomain(Object value1, JoinValueTypes type1, Object value2, JoinValueTypes type2) throws Exception {
        // Check the type between the values in the join parameters
        if (type1 != type2 || !value1.equals(value2))
            throw new BadRequestException("Check the type between the values in the join parameters");
    }


    //Post process utils

    private void createKey(List<DSDColumn> keyColumns, List<DSDColumn> valueColumns, DSDDataset[] dsdList, JoinParameter[][] joinParameters, String[][] valueParameters) {
        try {
            for (int r = 0; r < dsdList.length; r++) {
                List<String> joinId = new ArrayList<>();
                for (JoinParameter joinParameter : joinParameters[r])
                    joinId.add(joinParameter.getType() == JoinValueTypes.id ? (String) joinParameter.getValue() : null);
                Set<String> valuesId = new HashSet<>(Arrays.asList(valueParameters[r]));

                for (DSDColumn column : dsdList[r].getColumns())
                    if (column.getKey()) {
                        int index = joinId.indexOf(column.getId());
                        if (index >= 0)
                            keyColumns.get(index).setKey(true);
                        else if (!valuesId.contains(column.getId()))
                            throw new Exception();
                    }
            }
        } catch (Exception ex) {
            for (DSDColumn column : keyColumns)
                column.setKey(false);
            for (DSDColumn column : valueColumns)
                column.setKey(false);
        }

    }

    private void createSubjects(List<DSDColumn> keyColumns, List<DSDColumn> valueColumns, DSDDataset[] dsdList, JoinParameter[][] joinParameters, String[][] valueParameters) {

        Set<String> blacklistSubject = new HashSet<>();
        //dataset subjects analysis
        Map<String, Integer> subjectsIndex = new HashMap<>();
        for (int r = 0; r < dsdList.length; r++) {
            List<String> joinId = new ArrayList<>();
            for (JoinParameter joinParameter : joinParameters[r])
                joinId.add(joinParameter.getType() == JoinValueTypes.id ? (String) joinParameter.getValue() : null);
            List<String> valuesId = Arrays.asList(valueParameters[r]);

            for (DSDColumn column : dsdList[r].getColumns())
                if (column.getSubject() != null) {
                    int index = joinId.indexOf(column.getId());
                    if (index >= 0)
                        keyColumns.get(index).setSubject(column.getSubject());
                    else
                        index = valuesId.indexOf(column.getId());
                    if (index >= 0) {
                        Integer oldIndex = subjectsIndex.put(column.getSubject(), index);
                        if (oldIndex != null && oldIndex != index)
                            blacklistSubject.add(column.getSubject());
                    }
                }
        }

        for (DSDColumn column : keyColumns)
            if (blacklistSubject.contains(column.getSubject()))
                column.setSubject(null);
        for (DSDColumn column : valueColumns)
            if (blacklistSubject.contains(column.getSubject()))
                column.setSubject(null);

    }

    private String createQuery(Step[] steps, JoinParameter[][] joinParameters, String[][] valueParameters, Collection<Object> parameters, List<DSDColumn> keyColumns) {
        //retrieve tables name
        String[] tablesName = new String[steps.length];
        for (int i = 0; i < steps.length; i++)
            tablesName[i] = steps[i].getType() == StepType.table ? (String) steps[i].getData() : steps[i].getRid().getId();
        //Create select
        String[] joinColumnsName = new String[joinParameters[0].length];
        Object[] joinColumnsValues = new Object[joinParameters[0].length];
        StringBuilder select = new StringBuilder("SELECT ");

        // select key join columns
        for (int c = 0; c < joinColumnsName.length; c++) {
            boolean columnFound = false;
            for (int r = 0; r < joinParameters.length; r++) {
                if (joinParameters[r][c].getType() == JoinValueTypes.id && !columnFound) {
                    select.append(joinColumnsName[c] = tablesName[r] + '.' + joinParameters[r][c].getValue()).append(',');
                    columnFound = true;
                } else {
                    joinColumnsValues[c] = joinParameters[r][c].getValue();
                }
            }
        }
        // select value columns
        for (int r = 0; r < valueParameters.length; r++)
            for (int c = 0; c < valueParameters[r].length; c++)
                if (valueParameters[r][c] != null)
                    select.append(tablesName[r] + '.' + valueParameters[r][c]).append(',');
        select.setLength(select.length() - 1);

        //create join
        select.append(" FROM ").append(steps[0].getType() == StepType.table ? (String) steps[0].getData() : '(' + (String) steps[0].getData() + ") as " + steps[0].getRid().getId());
        for (int r = 1; r < joinParameters.length; r++) {
            select.append(" JOIN ").append(steps[r].getType() == StepType.table ? (String) steps[r].getData() : '(' + (String) steps[r].getData() + ") as " + steps[r].getRid().getId()).append(" ON (");
            if (steps[r].getType() == StepType.query) {
                Object[] existingParams = ((QueryStep) steps[r]).getParams();
                if (existingParams != null && existingParams.length > 0)
                    parameters.addAll(Arrays.asList(existingParams));
            }

            // in every columns, if the first row is an id
            for (int c = 0; c < joinParameters[r].length; c++) {
                if (joinParameters[0][c].getType() == JoinValueTypes.id) {
                    // add the names of the columns where type is id
                    select.append(joinColumnsName[c]).append(" = ");
                    if (joinParameters[r][c].getType() == JoinValueTypes.id) {
                        select.append(tablesName[r]).append('.').append(joinParameters[r][c].getValue());
                    } else
                    // otherwise add a parameter
                    {
                        select.append('?');
                        parameters.add(joinParameters[r][c].getValue());
                    }
                }
                // otherwise if the first row does not contain an id
                else if (joinParameters[r][c].getType() == JoinValueTypes.id) {
                    select.append("? = ").append(tablesName[r]).append('.').append(joinParameters[r][c].getValue());
                    parameters.add(joinColumnsValues[c]);
                }
                // add an AND if it is in the same row
                select.append(" AND ");
            }
            select.setLength(select.length() - 4);
            select.append(')');
        }

/*
        testFilter(select);
*/

        LOGGER.debug("QUERY : "+select.toString());
        return select.toString();
    }

    private void testFilter(StringBuilder select) {
        select.append(" limit 10 ");
    }


    //Pre process utils

    private DSDColumn updateId(DSDColumn column, String prefix) {
        DSDColumn result = column.clone();
        result.setId(prefix + "_" + column.getId());
        return result;
    }

    //Create the collection of DSD columns that are sepcified into the join parameters
    private Collection<DSDColumn> getValueColumns(Collection<DSDColumn> columns, JoinParameter[] joinParameters) {
        Set<String> keysName = new HashSet<>();
        for (JoinParameter joinParameter : joinParameters)
            if (joinParameter.getType() == JoinValueTypes.id)
                keysName.add((String) joinParameter.getValue());
        Collection<DSDColumn> valueColumns = new LinkedList<>();
        for (DSDColumn column : columns)
            if (!keysName.contains(column.getId()))
                valueColumns.add(column);
        return valueColumns;
    }

    //Create the collection of DSD columns that are specified into the join parameters
    private Collection<DSDColumn> getValueColumns(Collection<DSDColumn> columns, String[] rowValueParameters) {
        Set<String> keysName = new HashSet<>();
        for (String valueColumn : rowValueParameters)
            keysName.add(valueColumn);
        Collection<DSDColumn> valueColumns = new LinkedList<>();
        for (DSDColumn column : columns)
            if (keysName.contains(column.getId()))
                valueColumns.add(column);
        return valueColumns;
    }


}
