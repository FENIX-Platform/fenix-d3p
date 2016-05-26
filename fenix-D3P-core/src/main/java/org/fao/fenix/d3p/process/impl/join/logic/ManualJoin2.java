package org.fao.fenix.d3p.process.impl.join.logic;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.OjCodeList;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.StringUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.JoinParameter;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.dto.JoinTypes;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ext.ExceptionMapper;
import java.util.*;

public class ManualJoin2 implements JoinLogic {
    private @Inject StepFactory stepFactory;

    @Override
    public Step process(Step[] sourceStep, DSDDataset[] dsdList, JoinParams params) throws Exception {

        //Create key DSD
        JoinParameter[][] joinParameters = params.getJoins();
        List<DSDColumn> keyColumns = new LinkedList<>();
        for (int c=0; c<joinParameters[0].length; c++)
            for (int r=0; r<joinParameters.length; r++)
                if (joinParameters[r][c].getType()==JoinTypes.id) {
                    keyColumns.add(dsdList[r].findColumn((String)joinParameters[r][c].getValue()));
                    break;
                }

        //Create values DSD
        List<DSDColumn> valueColumns = new LinkedList<>();
        String[][] valueParameters = params.getValues();
        if (valueParameters==null || valueParameters.length==0)
            valueParameters = new String[joinParameters.length][];
        for (int r=0; r<valueParameters.length; r++)
            if (valueParameters[r]!=null && valueParameters[r].length>0) {
                Collection<DSDColumn> datasetValueColumns = getValueColumns(dsdList[r].getColumns(),joinParameters[r]);
                Collection<String> datasetValueColumnsName = new LinkedList<>();
                for (DSDColumn datasetValueColumn : datasetValueColumns) {
                    datasetValueColumnsName.add(datasetValueColumn.getId());
                    valueColumns.add(updateId(datasetValueColumn, sourceStep[r].getRid().getId()));
                }
                valueParameters[r] = datasetValueColumnsName.toArray(new String[datasetValueColumnsName.size()]);
            } else
                for (int c = 0; c < valueParameters[r].length; c++)
                    valueColumns.add(updateId(dsdList[r].findColumn(valueParameters[r][c]), sourceStep[r].getRid().getId()));

        //Create result dsd
        Collection<DSDColumn> joinedColumns = new LinkedList<>();
        joinedColumns.addAll(keyColumns);
        joinedColumns.addAll(valueColumns);
        cleanDSD(keyColumns, valueColumns, dsdList, joinParameters, valueParameters);

        DSDDataset dsd = new DSDDataset();
        dsd.setColumns(joinedColumns);
        dsd.setContextSystem("D3P");

        //Create result
        Collection<Object> queryParameters = new LinkedList<>();
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(createQuery(sourceStep,joinParameters,valueParameters,queryParameters));
        step.setParams(queryParameters.toArray());
        step.setTypes(null);
        return step;
    }

    @Override
    public void validate(Step[] sourceStep, DSDDataset[] dsdList, JoinParams params) throws Exception {
        JoinParameter[][] joinParameters = params.getJoins();
        String[][] valueParameters = params.getValues();

        if (joinParameters==null || joinParameters.length==0)
            throw new BadRequestException();
        if (joinParameters.length!=sourceStep.length || (valueParameters!=null && valueParameters.length>0 && valueParameters.length!=sourceStep.length))
            throw new BadRequestException();

        for (int r=0; r<joinParameters.length; r++) {
            Set<String> ids = new HashSet<>();
            boolean valueRowId = false;

            for (int c=0; c<joinParameters[r].length; c++) {
                if (joinParameters[r][c].getType() == JoinTypes.id) {
                    if (!ids.add((String) joinParameters[r][c].getValue()))
                        throw new BadRequestException();
                    valueRowId = true;
                    if (dsdList[r].findColumn((String) joinParameters[r][c].getValue()) == null)
                        throw new BadRequestException();
                }
            }
            if (valueParameters!=null && valueParameters.length>0)
                for (int c=0; c<valueParameters[r].length; c++) {
                    if (!ids.add(valueParameters[r][c]))
                        throw new BadRequestException();
                    if (dsdList[r].findColumn(valueParameters[r][c]) == null)
                        throw new BadRequestException();
                }

            if (!valueRowId)
                throw new BadRequestException();
        }

        for (int c=0; c<joinParameters[0].length; c++) {
            Object value = null;
            JoinTypes type = null;
            DSDColumn column = null;
            boolean valueColumnId = false;

            for (int r=0; r<joinParameters.length; r++) {
                if (joinParameters[r][c].getType() == JoinTypes.id) {
                    valueColumnId = true;
                    DSDColumn currentColumn = dsdList[r].findColumn((String) joinParameters[r][c].getValue());
                    if (column!=null)
                        checkDomain(column, currentColumn);
                    else if (value!=null)
                        checkDomain(currentColumn, value, type);
                    else
                        column = currentColumn;
                } else {
                    if (column!=null)
                        checkDomain(column, joinParameters[r][c].getValue(), joinParameters[r][c].getType());
                    else if (value!=null)
                        checkDomain(value, type, joinParameters[r][c].getValue(), joinParameters[r][c].getType());
                    else {
                        value = joinParameters[r][c].getValue();
                        type = joinParameters[r][c].getType();
                    }
                }
            }

            if (!valueColumnId)
                throw new BadRequestException();
        }

        //chiave non vuota
        //dimensioni parametri adatta alla sorgente
        //id di colonna esistenti
        //verifica compatibilita' colonne chiave
        //verifica che non ci sono righe chiave con soli valori fissi
        //verifica che non ci sono colonne chiave con soli valori fissi
        //Verifica che tra joins e values non ci sono identificativi duplicati a livello di singolo dataset
        //Verificare che i valori espliciti nei parametri join siano uguali a livello di singola colonna
    }

    private void checkDomain(DSDColumn column1, DSDColumn column2) throws Exception {
        
    }
    private void checkDomain(DSDColumn column1, Object value, JoinTypes type) throws Exception {

    }
    private void checkDomain(Object value1, JoinTypes type1, Object value2, JoinTypes type2) throws Exception {

    }


    //Post process utils

    private void cleanDSD (List<DSDColumn> keyColumns, List<DSDColumn> valueColumns, DSDDataset[] dsdList, JoinParameter[][] joinParameters, String[][] valueParameters) {
        //dataset subjects analysis
        Map<String, Integer> subjectsIndex = new HashMap<>();
        try {
            for (int r = 0; r < dsdList.length; r++) {
                List<String> joinId = new ArrayList<>();
                for (JoinParameter joinParameter : joinParameters[r])
                    joinId.add(joinParameter.getType() == JoinTypes.id ? (String) joinParameter.getValue() : null);
                List<String> valuesId = Arrays.asList(valueParameters[r]);

                for (DSDColumn column : dsdList[r].getColumns())
                    if (column.getSubject() != null) {
                        int index = joinId.indexOf(column.getId());
                        if (index>=0)
                            keyColumns.get(index).setSubject(column.getSubject());
                        else
                            index = valuesId.indexOf(column.getId());

                        Integer oldIndex = subjectsIndex.put(column.getSubject(), valuesId.indexOf(column.getId()));
                        if (oldIndex!=null && oldIndex!=index)
                            throw new Exception();
                    }
            }
        } catch (Exception ex) {
            for (DSDColumn column : keyColumns)
                column.setSubject(null);
            for (DSDColumn column : valueColumns)
                column.setSubject(null);
        }

        //dataset key analysis
        try {
            for (int r = 0; r < dsdList.length; r++) {
                List<String> joinId = new ArrayList<>();
                for (JoinParameter joinParameter : joinParameters[r])
                    joinId.add(joinParameter.getType() == JoinTypes.id ? (String) joinParameter.getValue() : null);
                Set<String> valuesId = new HashSet<>(Arrays.asList(valueParameters[r]));

                for (DSDColumn column : dsdList[r].getColumns())
                    if (column.getKey()) {
                        int index = joinId.indexOf(column.getId());
                        if (index>=0)
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



    private String createQuery(Step[] steps, JoinParameter[][] joinParameters, String[][] valueParameters, Collection<Object> parameters) {
        //retrieve tables name
        String[] tablesName = new String[steps.length];
        for (int i=0; i<steps.length; i++)
            tablesName[i] = steps[i].getType()==StepType.table ? (String)steps[i].getData() : steps[i].getRid().getId();
        //Create select
        String[] joinColumnsName = new String[joinParameters[0].length];
        Object[] joinColumnsValues = new Object[joinParameters[0].length];
        StringBuilder select = new StringBuilder("SELECT ");
        for (int c=0; c<joinColumnsName.length; c++)
            for (int r=0; r<joinParameters.length; r++)
                if (joinParameters[r][c].getType()==JoinTypes.id)
                    select.append(joinColumnsName[c] = tablesName[r]+'.'+joinParameters[r][c].getValue()).append(',');
                else
                    joinColumnsValues[c] = joinParameters[r][c].getValue();
        for (int r=0; r<valueParameters.length; r++)
            for (int c=0; c<valueParameters[r].length; c++)
                select.append(tablesName[r]+'.'+valueParameters[r][c]).append(',');
        select.setLength(select.length()-1);
        //create join
        select.append(" FROM ").append(steps[0].getType()==StepType.table ? (String)steps[0].getData() : '('+(String)steps[0].getData()+") as " + steps[0].getRid().getId());
        for (int r=1; r<joinParameters.length; r++) {
            select.append(" JOIN ").append(steps[r].getType()==StepType.table ? (String)steps[r].getData() : '('+(String)steps[r].getData()+") as " + steps[r].getRid().getId()).append(" ON (");
            if (steps[r].getType()==StepType.query) {
                Object[] existingParams = ((QueryStep) steps[r]).getParams();
                if (existingParams != null && existingParams.length > 0)
                    parameters.addAll(Arrays.asList(existingParams));
            }

            for (int c = 0; c < joinParameters[r].length; c++)
                if (joinParameters[0][c].getType()==JoinTypes.id) {
                    select.append(joinColumnsName[c]).append(" = ");
                    if (joinParameters[r][c].getType()==JoinTypes.id) {
                        select.append(tablesName[r]).append('.').append(joinParameters[r][c].getValue());
                    } else {
                        select.append('?');
                        parameters.add(joinParameters[r][c].getValue());
                    }
                } else if(joinParameters[r][c].getType()==JoinTypes.id) {
                    select.append("? = ").append(tablesName[r]).append('.').append(joinParameters[r][c].getValue());
                    parameters.add(joinColumnsValues[c]);
                }

            select.setLength(select.length()-4);
            select.append(')');
        }

        return select.toString();
    }


    //Pre process utils

    private DSDColumn updateId(DSDColumn column, String prefix) {
        column.setId(prefix+"_"+column.getId());
        return column;
    }

    private Collection<DSDColumn> getValueColumns(Collection<DSDColumn> columns, JoinParameter[] joinParameters) {
        Set<String> keysName = new HashSet<>();
        for (JoinParameter joinParameter : joinParameters)
            if (joinParameter.getType()==JoinTypes.id)
                keysName.add((String)joinParameter.getValue());
        Collection<DSDColumn> valueColumns = new LinkedList<>();
        for (DSDColumn column : columns)
            if (!keysName.contains(column.getId()))
                valueColumns.add(column);
        return valueColumns;
    }

}
