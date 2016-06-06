package org.fao.fenix.d3p.process.impl;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.log4j.Logger;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.AddColumnMap;
import org.fao.fenix.d3p.process.dto.AddColumnParams;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;
import org.fao.fenix.d3p.process.impl.join.JoinLogicFactory;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@ProcessName("addcolumn")
public class AddColumn extends org.fao.fenix.d3p.process.Process<AddColumnParams> {

    private static final Logger LOGGER = Logger.getLogger(AddColumn.class);
    @Inject
    JoinLogicFactory logicFactory;
    private
    @Inject
    StepFactory stepFactory;


    @Override
    public Step process(AddColumnParams params, Step... sourceStep) throws Exception {

        validate(params, sourceStep);
        Step source = sourceStep[0];
        //!) DSD
        //TODO check if column is addeable

        Object value = params.getValue();

        // query
        StringBuilder query = new StringBuilder("SELECT ");
        for (DSDColumn column : source.getCurrentDsd().getColumns())
            query.append(column.getId() + ",");

        if (value instanceof AddColumnMap) {
            // check the conditions
            query.append(" CASE ");
            AddColumnMap valueMap = (AddColumnMap) value;

            Object[] keys = valueMap.getKeys();

            for (int i = 0; i < keys.length; i++) {

                Object key = keys[i];

                if (key instanceof AddColumnMap) {
                    // second level
                    AddColumnMap secondLevelMap = (AddColumnMap) key;
                    Object[] keysSecondLevel = ((AddColumnMap) key).getKeys();

                    query.append("WHEN ");
                    for (int j = 0; j < keysSecondLevel.length; j++) {
                        query.append(keysSecondLevel[j].toString());
                        String valueToAppend = secondLevelMap.getValues()[j] == null ?
                                " IS NULL" :
                                " =" + secondLevelMap.getValues()[j].toString();
                        query.append(valueToAppend);
                    }
                } else if (key instanceof String) {
                    query.append(key);
                    String valueToAppend = valueMap.getValues()[i] == null ?
                            " IS NULL" :
                            " =" + valueMap.getValues()[i].toString();
                    query.append(valueToAppend);

                } else if (key == null) {
                    query.append(" END ");
                    break;
                } else {
                    throw new BadRequestException("error key");
                }
            }
        } else if (value instanceof String || value instanceof Integer || value instanceof Boolean || value instanceof Double) {
            // direct values

            String directValue = (value instanceof String) ? "\'"+value.toString()+"\'": value.toString();
            query.append(directValue + " AS " + params.getColumn().getId());
        }
        else {
            throw new UnsupportedOperationException("datatype not supported for value parameter");
        }
        query.append(" FROM "+ source.getData().toString());

        DSDDataset dsd = source.getDsd();
        dsd.getColumns().add(params.getColumn());
        QueryStep step = (QueryStep) stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        dsd.setContextSystem("D3P");
        step.setData(query.toString());
        step.setTypes(null);
        return step;
    }

    // Validation
    private void validate(AddColumnParams params, Step... sourceSteps) {
        initialValidation(sourceSteps);
        // TODO validate params with source

        //column parameter validation
        Step source = sourceSteps[0];
        for (DSDColumn column : source.getDsd().getColumns()) {
            if (params.getColumn().getId().equals(column.getId()))
                throw new BadRequestException("id equals");
            if (params.getColumn().getSubject()!= null && column.getSubject()!= null && params.getColumn().getSubject().equals(column.getSubject()))
                throw new BadRequestException("subject equals");
        }
    }

    private void initialValidation(Step... sourceSteps) {
        Step source = sourceSteps != null && sourceSteps.length == 1 ? sourceSteps[0] : null;
        if (source == null)
            throw new BadRequestException("there is not a source specified");
        // check storage is the same and query type is table or query
        DatasetStorage storage = source.getStorage();
        StepType type = source.getType();
        // ofr now is not possible to have an iterator step
        if (type == null || ((type != StepType.table) && ((type != StepType.query))))
            throw new UnsupportedOperationException("add column process can only be applied on a table or on other select query");
    }
}




