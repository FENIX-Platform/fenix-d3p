package org.fao.fenix.d3p.process.impl.join.logic;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.OjCodeList;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.JoinParameter;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.dto.JoinTypes;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;

import javax.ws.rs.BadRequestException;
import java.util.*;

public class ManualJoin implements JoinLogic {

    private JoinParams params;

    private Step[] steps;
    private Map<String, Set<String>> keyColumns;
    private Map<String, Set<String>> otherColumns;
    private Map<String, Collection<String>> uidBlacklist;
    private Map<Integer, Set<Integer>> keyColumnsPosition;
    private HashMap<Integer, Set<Integer>> otherColumnsPosition;

    public ManualJoin(JoinParams params) {
        this.params = params;
    }


    @Override
    public Step process(Step... sourceStep) throws Exception {

        // check that parameters follows the sid
        this.keyColumns = new HashMap<String, Set<String>>();
        this.keyColumnsPosition = new HashMap<Integer, Set<Integer>>();
        this.otherColumns = new HashMap<String, Set<String>>();
        this.otherColumnsPosition = new HashMap<Integer, Set<Integer>>();

        this.uidBlacklist = new HashMap<String, Collection<String>>();
        this.steps = sourceStep;

        createDSDColumns();

        StringBuilder query;

        return null;
    }

    /**
     * Check if the number of parameters is the same for each resource
     *
     * @param steps
     */
    @Override
    public void validate(Step... steps) throws Exception {
        if (this.params.getJoins().size() == steps.length) {
            Iterator<Collection<JoinParameter>> it = this.params.getJoins().iterator();
            int sizeFirst = it.next().size();
            while (it.hasNext()) {
                if (sizeFirst != it.next().size()) {
                    throw new BadRequestException("wrong configuration for join parameters: the number should be the same for each resource");
                }
            }
        } else {
            throw new BadRequestException("wrong configuration for join parameters: the number should be equal to the number of resources");
        }
    }


    private List<DSDColumn> createDSDColumns() {

        // create key Columns
        List<DSDColumn> resultColumns = new ArrayList<DSDColumn>();
        fillKeyColumns();
        fillOtherColumns();
        return resultColumns;
    }


    private void fillKeyColumns() {

        ArrayList<Collection<JoinParameter>> parameters = (ArrayList<Collection<JoinParameter>>) this.params.getJoins();
        // for each parameter
        for (int i = 0, keySize = ((ArrayList<JoinParameter>) parameters.get(0)).size(); i < keySize; i++) {
            // search id for each dataset
            boolean columnNotFound = true;
            Collection<JoinParameter> selectAsParameters = new ArrayList<JoinParameter>();
            //for each dataset
            for (int j = 0, sidLength = parameters.size(); j < sidLength && columnNotFound; j++) {

                JoinParameter joinParameter = ((ArrayList<JoinParameter>) parameters.get(j)).get(i);
                if (joinParameter.getType() == JoinTypes.id) {
                    DSDColumn column = steps[j].getCurrentDsd().findColumn(joinParameter.getValue().toString());
                    if (column != null) {

                        columnNotFound = !checkDuplicatedAndInsertKeyColumns(i, j, column, steps);
                    } else {
                        throw new BadRequestException("wrong configuration for columns: id " + joinParameter.getValue().toString() + " does not exist");
                    }
                } else {
                    selectAsParameters.add(joinParameter);
                }
            }
        }

        if (keyColumns.size() <= 0)
            throw new BadRequestException("wrong configuration for join parameters:id parameters do not exist");
    }


    private boolean isColumnJoinableWithType(DSDColumn source, JoinParameter destination) {

        if ((destination.getType() == JoinTypes.code || destination.getType() == JoinTypes.text) &&
                destination.getValue() instanceof String) {
            DataType dataType = source.getDataType();

            return dataType != DataType.bool && dataType != DataType.number && dataType != DataType.date && dataType != DataType.month &&
                    dataType != DataType.year && dataType != DataType.time && dataType != DataType.number;
        }
        // boolean
        else if (destination.getType() == JoinTypes.bool) {
            return source.getDataType() == DataType.bool;
        }
        // number

        return true;

    }


    private boolean areColumnsJoinable(DSDColumn source, DSDColumn destination) {
        boolean result = false;
        DataType sourceDatatype = source.getDataType();
        result = sourceDatatype == destination.getDataType();
        if (result) {
            if (sourceDatatype == DataType.code) {
                result = (((List<OjCodeList>) source.getDomain().getCodes()).get(0).getIdCodeList()) == (((List<OjCodeList>) destination.getDomain().getCodes()).get(0).getIdCodeList());
                if (((List<OjCodeList>) source.getDomain().getCodes()).get(0).getVersion() != null)
                    result = (((List<OjCodeList>) source.getDomain().getCodes()).get(0).getVersion()).toString().equals(((List<OjCodeList>) destination.getDomain().getCodes()).get(0).getVersion());

            }
        }
        return result;
    }


    private boolean checkDuplicatedAndInsertKeyColumns(int sourceColumnIndex, int sourceRowIndex, DSDColumn sourceColumn, Step[] steps) {
        ArrayList<Collection<JoinParameter>> parameters = (ArrayList<Collection<JoinParameter>>) this.params.getJoins();

        boolean compatible = true;
        for (int rowIndex = 0; rowIndex < steps.length && compatible; rowIndex++) {

            if (rowIndex != sourceRowIndex) {
                JoinParameter param = ((ArrayList<JoinParameter>) parameters.get(rowIndex)).get(sourceColumnIndex);
                // if it is a column
                if (param.getType() == JoinTypes.id) {
                    DSDColumn destinationColumn = steps[rowIndex].getCurrentDsd().findColumn(param.getValue().toString());
                    // if it is not a blacklist column
                    if (!isABlacklistedColumn(steps[rowIndex].getRid().getId(), destinationColumn.getId())) {
                        compatible = areColumnsJoinable(sourceColumn, destinationColumn);
                        if (compatible) {
                            insertKeyColumns(steps[sourceRowIndex].getRid().getId(), steps[rowIndex].getRid().getId(), sourceColumn, destinationColumn);
                            updatePositionMatrix(rowIndex, sourceColumnIndex);
                        }
                    }
                } else {
                    compatible = isColumnJoinableWithType(sourceColumn, param);
                    if (compatible) {
                        insertKeyColumns(steps[sourceRowIndex].getRid().getUid(), sourceColumn);
                        updatePositionMatrix(rowIndex, sourceColumnIndex);
                    }
                }

            }
        }
        return compatible;
    }


    private void updatePositionMatrix(int rowIndex, int columnIndex) {
        Set<Integer> values = this.keyColumnsPosition.containsKey(rowIndex) ? this.keyColumnsPosition.get(rowIndex) : new HashSet<Integer>();
        values.add(columnIndex);
        this.keyColumnsPosition.put(rowIndex, values);
    }


    private void insertKeyColumns(String resourceUID, DSDColumn keyColumn) {

        Set<String> values = (keyColumns.containsKey(resourceUID)) ? keyColumns.get(resourceUID) : new HashSet<String>();
        values.add(keyColumn.getId());
        keyColumns.put(resourceUID, values);
        if (keyColumn.getDataType() == DataType.code)
            blacklistLabelColumns(keyColumn, resourceUID);
    }


    private void insertKeyColumns(String sourceUID, String destinationUID, DSDColumn sourceColumn, DSDColumn destinationColumn) {

        Set<String> values = (keyColumns.containsKey(sourceUID)) ? keyColumns.get(sourceUID) : new HashSet<String>();
        values.add(sourceColumn.getId());
        keyColumns.put(sourceUID, values);
        if (destinationColumn.getDataType() == DataType.code)
            blacklistLabelColumns(destinationColumn, destinationUID);
        blacklistLabelColumns(destinationColumn, destinationUID);
    }


    private boolean isABlacklistedColumn(String datasetUID, String columnID) {
        return uidBlacklist.containsKey(datasetUID) && uidBlacklist.get(datasetUID).contains(columnID);
    }


    private void blacklistLabelColumns(DSDColumn columnToBeRemoved, String UIDDataset) {

        fillBlacklistMap(UIDDataset, columnToBeRemoved.getId());
        for (int i = 0; i < steps.length; i++) {
            if (steps[i].getRid().getId() == UIDDataset) {
                ArrayList<DSDColumn> datasetColumns = (ArrayList<DSDColumn>) steps[i].getCurrentDsd().getColumns();
                for (DSDColumn column : datasetColumns) {
                    if (isLabelColumn(columnToBeRemoved.getId(), column.getId())) {
                        fillBlacklistMap(UIDDataset, column.getId());
                    }
                }
            }
        }
    }


    private void fillBlacklistMap(String key, String value) {
        ArrayList<String> values = (uidBlacklist.containsKey(key)) ? (ArrayList<String>) uidBlacklist.get(key) : new ArrayList<String>();
        values.add(value);
        uidBlacklist.put(key, values);
    }


    private boolean isLabelColumn(String originalColumnID, String possibleColumnID) {

        return (possibleColumnID.length() > originalColumnID.length()) &&
                (possibleColumnID.substring(0, originalColumnID.length() + 1) == originalColumnID);
    }


    private void fillOtherColumns( Step... sources) {

        // if are specified
        if(areValuesParameters(sources)) {
            ArrayList<Collection<String>> values = ( ArrayList<Collection<String>>)this.params.getValues();
            for(int i=0, datasetNumber = values.size(); i<datasetNumber; i++){
                for(String colum: values.get(i))
                    addOtherColumns(steps[i].getCurrentDsd(), i, -1, colum);
            }
        }else {
            for (int i = 0, sidSize = sources.length; i < sidSize; i++) {
                org.fao.fenix.commons.msd.dto.full.DSDDataset dataset = steps[i].getCurrentDsd();
                for (int j = 0, datasetSize = dataset.getColumns().size(); j < datasetSize; j++) {
                    // if it is not duplicated, add
                    if (!isADuplicateColumn(i, j) && !isABlacklistedColumn(dataset.getRID(),((ArrayList<DSDColumn>)dataset.getColumns()).get(j).getId())) {
                        addOtherColumns(dataset, i, j, null);
                    }
                }
            }
        }
    }


    private void addOtherColumns (DSDDataset sourceDataset,  int rowPosition, int columnPosition, String columnID) {
        String uid = sourceDataset.getRID();
        Set<String> values = (this.otherColumns.containsKey(uid)) ? this.otherColumns.get(uid) : new HashSet<String>();

        DSDColumn column = (columnID!= null)? sourceDataset.findColumn(columnID) : ((ArrayList<DSDColumn>)sourceDataset.getColumns()).get(columnPosition);
        if(column!= null)
            values.add(column.getId());
        else
            throw new BadRequestException("wrong configuration for values parameters: the "+columnID+ " id for the dataset "+sourceDataset.getRID()+ " does not exist");
        this.otherColumns.put(uid,values );

        if((Integer)columnPosition != null) {
            Set<Integer> positionValues = (this.otherColumnsPosition.containsKey(rowPosition)) ? this.otherColumnsPosition.get(rowPosition) : new HashSet<Integer>();
            positionValues.add(columnPosition);
            this.otherColumnsPosition.put(rowPosition, positionValues);
        }
    }


    private boolean isADuplicateColumn(int row, int column) {
        return this.keyColumnsPosition.get(row) != null && this.keyColumnsPosition.get(row).contains(column);
    }


    private boolean areValuesParameters(Step... source) {
        if (this.params.getValues() != null && this.params.getValues().size() > 0) {
            if (source.length == this.params.getValues().size()) {
                return true;
            } else {
                throw new BadRequestException("wrong configuration for values parameter : number of values should follow the number of dataset");
            }
        }
        return false;
    }

}
