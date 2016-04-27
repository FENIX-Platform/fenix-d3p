package org.fao.fenix.d3p.process.impl.join.logic;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.OjCodeList;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.JoinParameter;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.dto.JoinTypes;
import org.fao.fenix.d3p.process.impl.Join;
import org.fao.fenix.d3p.process.impl.Page;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;

import javax.ws.rs.BadRequestException;
import javax.xml.crypto.Data;
import java.util.*;

public class ManualJoin implements JoinLogic {

    private JoinParams params;

    private Step[] steps;
    private Map<String, Set<String>> keyColumns;
    private Map<String, Set<String>> otherColumns;
    private Map<String, Collection<String>> uidBlacklist;

    public ManualJoin(JoinParams params) {
        this.params = params;
    }


    @Override
    public Step[] process(Step... sourceStep) {

        // check that parameters follows the sid
        this.keyColumns = new HashMap<String, Set<String>>();
        this.otherColumns = new HashMap<String, Set<String>>();
        this.uidBlacklist = new HashMap<String, Collection<String>>();
        this.steps = sourceStep;

        validate(sourceStep);
        createDSDColumns(sourceStep);

        StringBuilder query;


        return new Step[0];
    }

    private void validate(Step... steps) {
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

    private List<DSDColumn> createDSDColumns(Step... sourceStep) {

        // create key Columns
        Map<Integer, Set<Integer>> positionKeyColumnsForDataset = new HashMap<Integer, Set<Integer>>();
        List<DSDColumn> resultColumns = new ArrayList<DSDColumn>();
        fillKeyColumns();
        fillOtherColumns(resultColumns, positionKeyColumnsForDataset, sourceStep);
        return null;

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

    private boolean areParametersJoinable(JoinParameter source, JoinParameter destination) {

        return source.getType() == destination.getType();
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


    private void fillKeyColumns() {


        ArrayList<Collection<JoinParameter>> parameters = (ArrayList<Collection<JoinParameter>>) this.params.getJoins();
        // for each parameter
        for (int i = 0, keySize = ((ArrayList<JoinParameter>) parameters.get(0)).size(); i < keySize; i++) {
            // search id for each dataset
            boolean columnNotFound = true;
            //for each dataset
            for (int j = 0, sidLength = parameters.size(); j < sidLength && columnNotFound; j++) {
                ArrayList<JoinParameter> parameterList = (ArrayList<JoinParameter>) parameters.get(j);
                if (parameterList.get(i).getType() == JoinTypes.id) {
                    DSDColumn column = find(parameterList.get(i).getValue().toString(), steps[j].getDsd());
                    if (column != null)
                        columnNotFound = !checkCompatibilityKeyColumns(i, j, column, steps);

                }
            }
        }

        if (keyColumns.size() <= 0)
            throw new BadRequestException("wrong configuration for join parameters:id parameters do not exist");
    }

    private boolean checkCompatibilityKeyColumns(int sourceColumnIndex, int sourceRowIndex, DSDColumn sourceColumn, Step[] steps) {
        ArrayList<Collection<JoinParameter>> parameters = (ArrayList<Collection<JoinParameter>>) this.params.getJoins();

        boolean compatible = true;
        for (int rowIndex = 0; rowIndex < steps.length && compatible; rowIndex++) {

            if (rowIndex != sourceRowIndex) {
                JoinParameter param = ((ArrayList<JoinParameter>) parameters.get(rowIndex)).get(sourceColumnIndex);
                // if it is a column and it is not blacklisted
                if (param.getType() == JoinTypes.id ) {
                    DSDColumn destinationColumn =  steps[rowIndex].getCurrentDsd().findColumn(param.getValue().toString());
                     // if it is not a blacklist column
                    if(!isABlacklistedColumn(steps[rowIndex].getRid().getId(), destinationColumn.getId())) {
                        compatible = areColumnsJoinable(sourceColumn, destinationColumn);
                        if (compatible)
                            insertSupportMapColumns(steps[sourceRowIndex].getRid().getId(), steps[rowIndex].getRid().getId(), sourceColumn, destinationColumn);

                    }
                } else {

                    compatible = isColumnJoinableWithType(sourceColumn,param);

                }

            }
        }
        return compatible;
    }

    private boolean isABlacklistedColumn(String datasetUID, String columnTitle) {
        return uidBlacklist.containsKey(datasetUID) && uidBlacklist.get(datasetUID).contains(columnTitle);
    }


    private void insertSupportMapColumns(String sourceUID, String destinationUID, DSDColumn sourceColumn, DSDColumn destinationColumn) {


        Set<String> values = (keyColumns.containsKey(sourceUID)) ? keyColumns.get(sourceUID) : new HashSet<String>();
        values.add(sourceColumn.getId());
        keyColumns.put(sourceUID, values);
        if (destinationColumn.getDataType() == DataType.code)
            blacklistLabelColumns(destinationColumn, destinationUID);
        blacklistLabelColumns(destinationColumn, destinationUID);


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



    private DSDColumn find(String columnID, org.fao.fenix.commons.msd.dto.full.DSDDataset dsdDataset) {
        for (DSDColumn column : dsdDataset.getColumns()) {
            if (column.getId().equals(columnID))
                return column;
        }
        throw new BadRequestException("wrong configuration for join parameters: id does not exists into that dataset");
    }

    private void fillOtherColumns() {

        Map<Integer, Set<String>> valuesToBeShown = (areValuesParameters(steps)) ? createMapValues() : null;


        for (int i = 0, sidSize = steps.length; i < sidSize; i++) {

        }

        /*Map<Integer, Set<String>> valuesToBeShown = (areValuesParameters(steps)) ? createMapValues() : null;
        for (int i = 0, sidSize = steps.length; i < sidSize; i++) {
            org.fao.fenix.commons.msd.dto.full.DSDDataset dataset = sources[i].getDsd();
            for (int j = 0, datasetSize = dataset.getColumns().size(); j < datasetSize; j++) {
                // if it is not duplicated, add
                if (!isADuplicateColumn(i, j, positionKeyColumns) ||
                        (valuesToBeShown != null &&
                                valuesToBeShown.get(i).contains(((ArrayList<DSDColumn>) dataset.getColumns()).get(j).getId()))) {
                    columns.add(((ArrayList<DSDColumn>) dataset.getColumns()).get(j));
                }
            }
        }*/
    }


    private void fillOtherColumns(List<DSDColumn> columns, Map<Integer, Set<Integer>> positionKeyColumns, Step... sources) {

        Map<Integer, Set<String>> valuesToBeShown = (areValuesParameters(sources)) ? createMapValues() : null;
        for (int i = 0, sidSize = sources.length; i < sidSize; i++) {
            org.fao.fenix.commons.msd.dto.full.DSDDataset dataset = sources[i].getDsd();
            for (int j = 0, datasetSize = dataset.getColumns().size(); j < datasetSize; j++) {
                // if it is not duplicated, add
                if (!isADuplicateColumn(i, j, positionKeyColumns) ||
                        (valuesToBeShown != null &&
                                valuesToBeShown.get(i).contains(((ArrayList<DSDColumn>) dataset.getColumns()).get(j).getId()))) {
                    columns.add(((ArrayList<DSDColumn>) dataset.getColumns()).get(j));
                }
            }
        }
    }

    private boolean isADuplicateColumn(int row, int column, Map<Integer, Set<Integer>> positionKeyColumns) {
        return positionKeyColumns.get(row) != null && positionKeyColumns.get(row).contains(column);
       /*if(positionKeyColumns.get(row) != null && positionKeyColumns.get(row).contains(column)){
           this.uidBlacklist = (this.uidBlacklist== null)? new HashMap<Integer, Collection<String>>(): this.uidBlacklist.containsKey(row)
       };*/
    }


    private boolean areValuesParameters(Step... source) {
        if (this.params.getValues() != null && this.params.getValues().size() > 0) {
            if (source.length == this.params.getValues().size()) {
                return true;

            } else {
                throw new BadRequestException("wrong configuration for values parameter : number of values should follow the number of dataset");
            }
        } else {
            return false;
        }
    }

    private Map<Integer, Set<String>> createMapValues() {
        Map<Integer, Set<String>> result = new HashMap<Integer, Set<String>>();
        ArrayList<Collection<String>> values = (ArrayList<Collection<String>>) this.params.getValues();
        for (int i = 0, size = values.size(); i < size; i++) {
            Set<String> ids = new HashSet<String>();
            for (String s : values.get(i))
                ids.add(s);
            result.put(i, ids);
        }
        return result;
    }


/*
    private void fillSupportMap(DSDColumn column, String datasetUID, Map<String, Set<String>> map) {


        Set<String> values = (map.containsKey(datasetUID) && map.get(datasetUID) != null && map.get(datasetUID).size() > 0) ?
                new HashSet<String>(map.get(datasetUID)) :
                new HashSet<String>();
        values.add(column.getId());
        map.put(datasetUID, values);

    }
*/

}
