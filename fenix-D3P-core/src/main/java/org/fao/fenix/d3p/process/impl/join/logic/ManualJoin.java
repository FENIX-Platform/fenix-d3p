package org.fao.fenix.d3p.process.impl.join.logic;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.JoinParameter;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.dto.JoinTypes;
import org.fao.fenix.d3p.process.impl.Join;
import org.fao.fenix.d3p.process.impl.Page;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;

import javax.ws.rs.BadRequestException;
import java.util.*;

public class ManualJoin implements JoinLogic {

    private JoinParams params;

    public ManualJoin(JoinParams params) {
        this.params = params;
    }


    @Override
    public Step[] process(Step... sourceStep) {

        // check that parameters follows the sid

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
        fillKeyColumns(resultColumns, positionKeyColumnsForDataset, sourceStep);
        fillOtherColumns(resultColumns, positionKeyColumnsForDataset, sourceStep);
        return null;

    }

    private void fillKeyColumns(List<DSDColumn> columns, Map<Integer, Set<Integer>> positionKeyColumns, Step... sources) {

        ArrayList<Collection<JoinParameter>> parameters = (ArrayList<Collection<JoinParameter>>) this.params.getJoins();
        for (int i = 0, keyColumns = ((ArrayList<JoinParameter>) parameters.get(0)).size(); i < keyColumns; i++) {
            boolean idFound = false;
            // search id for each dataset
            for (int j = 0, sidLength = parameters.size(); j < sidLength; j++) {
                ArrayList<JoinParameter> parameterList = (ArrayList<JoinParameter>) parameters.get(j);
                if (parameterList.get(i).getType() == JoinTypes.id) {
                    if (idFound == false) {
                        DSDColumn column = find(parameterList.get(i).getValue().toString(), sources[j].getDsd());
                        if (column != null) {
                            setPositionKey(positionKeyColumns, j, i);
                            columns.add(column);
                            idFound = true;
                        }
                    } else {
                        setPositionKey(positionKeyColumns, j, i);
                    }
                }
            }
        }

        if (columns.size() <= 0)
            throw new BadRequestException("wrong configuration for join parameters:id parameters does not exists");
    }

    private void setPositionKey(Map<Integer, Set<Integer>> positions, int key, int value) {
        Set<Integer> values = (HashSet<Integer>) positions.get(key);
        values = values == null ? new HashSet<Integer>() : values;
        values.add(value);
        positions.put(key, values);

    }


    private DSDColumn find(String columnID, org.fao.fenix.commons.msd.dto.full.DSDDataset dsdDataset) {
        for (DSDColumn column : dsdDataset.getColumns()) {
            if (column.getId().equals(columnID))
                return column;
        }
        throw new BadRequestException("wrong configuration for join parameters: id does not exists into that dataset");
    }

    private void fillOtherColumns(List<DSDColumn> columns, Map<Integer, Set<Integer>> positionKeyColumns, Step... sources) {

        Map<Integer, Set<String>> valuesToBeShown = (areValuesParameters(sources)) ? createMapValues() : null;
        for (int i = 0, sidSize = sources.length; i < sidSize; i++) {
            org.fao.fenix.commons.msd.dto.full.DSDDataset dataset = sources[i].getDsd();
            for (int j = 0, datasetSize = dataset.getColumns().size(); j < datasetSize; j++) {
                // if it is not duplicated, add
                if (!isADuplicateColumn(i, j, positionKeyColumns) ||
                        (valuesToBeShown != null && valuesToBeShown.get(i).contains(((ArrayList<DSDColumn>) dataset.getColumns()).get(j).getId())) ) {
                        columns.add(((ArrayList<DSDColumn>) dataset.getColumns()).get(j));
                }
            }
        }
    }

    private boolean isADuplicateColumn(int row, int column, Map<Integer, Set<Integer>> positionKeyColumns) {
        return positionKeyColumns.get(row) != null && positionKeyColumns.get(row).contains(column);
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

}
