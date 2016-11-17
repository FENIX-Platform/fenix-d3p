package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.VariableFilter;
import org.fao.fenix.commons.find.dto.type.VariableValueType;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.database.*;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Iterator;

@ProcessName("var")
public class Variable extends org.fao.fenix.d3p.process.Process<VariableFilter> {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;
    private @Inject Resources resourcesService;




    @Override
    public Step process(VariableFilter params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length>0 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("Variable filter can be applied only on a table or an other select query");
        String tableName = type==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();
        //Retrieve variables values
        Map<String, Object[]> variables = getVariablesValues(params,source,type,tableName,dsd);
        //Set values
        if (params.isGlobal())
            for (org.fao.fenix.commons.find.dto.filter.Variable variable : params.getVariables())
                setGlobalVariable(variable.getKey(), variables.get(variable.getKey()));
        else
            for (org.fao.fenix.commons.find.dto.filter.Variable variable : params.getVariables())
                setChannelVariable(variable.getKey(), variables.get(variable.getKey()));

        //Return a copy of the source step
        Step step = stepFactory.getInstance(type);
        step.setDsd(dsd);
        step.setData(source.getData());
        if (type==StepType.query) {
            ((QueryStep)step).setParams(((QueryStep)source).getParams());
            ((QueryStep)step).setTypes(((QueryStep)source).getTypes());
        }
        return step;
    }

    //Logic
    private Map<String, Object[]> getVariablesValues(VariableFilter params, Step source, StepType sourceType, String tablaName, DSDDataset dsd) throws Exception {
        DSDColumn[] columns = dsd.getColumns().toArray(new DSDColumn[dsd.getColumns().size()]);
        org.fao.fenix.commons.find.dto.filter.Variable[] variables = params.getVariables().toArray(new org.fao.fenix.commons.find.dto.filter.Variable[params.getVariables().size()]);

        Map<String,Integer> indexes = new LinkedHashMap<>();
        Map<String,String> sqls = new LinkedHashMap<>();
        Map<String,Collection> variablesMatrix = new LinkedHashMap<>();
        Map<String, Object[]> values = new HashMap<>();

        //Analyze parameters and take constant values
        for (int i=0; i<variables.length; i++) {
            if (variables[i].getType()==VariableValueType.constant) { //fixed value
                values.put(variables[i].getKey(), new Object[]{variables[i].getValue()});
            } else { //dynamic value
                variablesMatrix.put(variables[i].getKey(), new LinkedList());
                if (sourceType==StepType.iterator) { //id for an iterator step
                    for (int columnIndex=0;columnIndex<columns.length;columnIndex++)
                        if (columns[columnIndex].getId().equals(String.valueOf(variables[i].getValue())))
                            indexes.put(variables[i].getKey(), columnIndex);
                } else { //sql or id for a query or table step
                    sqls.put(variables[i].getKey(), String.valueOf(variables[i].getValue()));
                }
            }
        }

        //Take dynamic values
        if (variablesMatrix.size()>0) {
            String[] keysArray = variablesMatrix.keySet().toArray(new String[variablesMatrix.size()]);
            Collection[] variablesMatrixArray = variablesMatrix.values().toArray(new Collection[variablesMatrix.size()]);

            for (Object[] data : loadData(tablaName,source,sourceType,sqls.values(),indexes.values().toArray(new Integer[indexes.size()])))
                for (int i=0; i<data.length; i++)
                    variablesMatrixArray[i].add(data[i]);

            for (int i=0; i<variablesMatrixArray.length; i++)
                values.put(keysArray[i], variablesMatrixArray[i].toArray());
        }

        return values;
    }



    private Collection<Object[]> loadData(String tableName, Step source, StepType sourceType, Collection<String> sqls, Integer[] indexes) throws Exception {
        Collection<Object[]> buffer = new LinkedList<>();
        Connection connection = source.getStorage().getConnection();
        try {
            switch (sourceType) {
                case iterator:
                    for (Iterator<Object[]> dataIterator = ((IteratorStep)source).getData(); dataIterator.hasNext(); ) {
                        Object[] rawRow = dataIterator.next();
                        Object[] row = new Object[indexes.length];
                        for (int i=0; i<indexes.length; i++)
                            row[i] = rawRow[indexes[i]];
                        buffer.add(row);
                    }
                    break;
                case query:
                    for (ResultSet resultSet = databaseUtils.fillStatement(connection.prepareStatement(createQuery(tableName,sqls)), ((QueryStep)source).getTypes(), ((QueryStep)source).getParams()).executeQuery(); resultSet.next();)
                        buffer.add(getRow(resultSet,sqls.size()));
                    break;
                case table:
                    for (ResultSet resultSet = connection.prepareStatement(createQuery(tableName,sqls)).executeQuery(); resultSet.next();)
                        buffer.add(getRow(resultSet,sqls.size()));
                    break;
            }
        } finally {
            connection.close();
        }
        return buffer;
    }

    private String createQuery (String tableName, Collection<String> sqls) {
        boolean empty = true;
        for (String sql : sqls)
            empty &= sql==null;
        if (empty)
            return "SELECT * FROM "+tableName;

        StringBuilder select = new StringBuilder();
        for (String column : sqls)
                select.append(',').append(column);
        return "SELECT "+select.substring(1)+" FROM "+tableName;
    }




    //Validation
    private void validate(VariableFilter params, StepType type, DSDDataset dsd) throws BadRequestException {
        if (params.getVariables()==null || params.getVariables().size()==0)
            throw new BadRequestException("In process 'var', 'variables' parameter must be filled.");

        for (org.fao.fenix.commons.find.dto.filter.Variable variable : params.getVariables()) {
            if (variable.getType() == null || variable.getKey() == null || variable.getKey().trim().length() == 0 || variable.getValue() == null)
                throw new BadRequestException("In process 'var', 'type', 'key' and 'value' attributes are mandatory for any 'variables' array item declaration.");
            if (!(variable.getValue() instanceof String) && (variable.getType()==VariableValueType.id || variable.getType()==VariableValueType.sql))
                throw new BadRequestException("In process 'var', the variable 'value' attribute must be a string if the type is one of 'id' or 'sql'");
            if (variable.getType()==VariableValueType.sql && type!=StepType.query && type!=StepType.table)
                throw new BadRequestException("In process 'var', a 'sql' type variable 'value' can be used only with 'table' or 'query' source step");

            if (variable.getType()==VariableValueType.id && dsd.findColumn((String) variable.getValue())==null)
                throw new BadRequestException("In process 'var', specified column '"+variable.getValue()+"' cannot be found");
        }
    }

    //Utils
    private Object[] getRow(ResultSet resultSet, int columnsCount) throws SQLException {
        Object[] row = new Object[columnsCount];
        for (int i=0; i<columnsCount; i++)
            row[i] = resultSet.getObject(i+1);
        return row;
    }


}
