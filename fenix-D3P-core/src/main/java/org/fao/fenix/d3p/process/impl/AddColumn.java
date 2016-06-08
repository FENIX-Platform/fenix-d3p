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
import java.util.*;

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
        StringBuilder query = new StringBuilder("");
        // query

        query.append(" CASE ");
        if (value instanceof AddColumnMap) {
            // check the conditions
            AddColumnMap valueMap = (AddColumnMap) value;
            Object[] keysFirstLevel = valueMap.getKeys();
            Object[] valuesFirstLevel = valueMap.getValues();

            for (int i = 0; i < keysFirstLevel.length; i++) {
                Object key = keysFirstLevel[i];

                if (key instanceof AddColumnMap) {
                    // second level
                    AddColumnMap secondLevelMap = (AddColumnMap) key;
                    if (secondLevelMap == null || secondLevelMap.getKeys().length > 0) {
                        query.append(" ELSE NULL END");
                        break;
                    }

                    Object[] keysSecondLevel = ((AddColumnMap) key).getKeys();
                    query.append("WHEN ");
                    for (int j = 0; j < keysSecondLevel.length; j++) {
                        query.append(keysSecondLevel[j].toString());
                        String valueToAppend = secondLevelMap.getValues()[j] == null ?
                                " IS NULL" :
                                " =" + secondLevelMap.getValues()[j].toString();
                        query.append(valueToAppend);
                        query.append(" AND ");
                    }
                    query.setLength(query.length() - 5);
                    query.append("THEN " + valuesFirstLevel[i]);
                } else if (key instanceof String) {
                    query.append(key);
                    query.append(" THEN ");
                    query.append(valueMap.getValues()[i]);

                } else if (key == null) {
                    query.append(" ELSE NULL END");
                    break;
                } else {
                    throw new BadRequestException("error key");
                }
            }
        } else if (value instanceof String || value instanceof Integer || value instanceof Boolean || value instanceof Double) {
            // direct values

            query.append(" ELSE ");
            String directValue = (value instanceof String) ? "\'" + value.toString() + "\'" : value.toString();
            query.append(directValue + " END");
        } else {
            throw new UnsupportedOperationException("datatype not supported for value parameter");
        }

        query.append(" AS "+ params.getColumn().getId());

        DSDDataset dsd = source.getDsd();
        Collection<DSDColumn> newColumns = dsd.getColumns();
        newColumns.add(params.getColumn());
        dsd.setColumns(newColumns);
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

        if (params.getColumn() == null)
            throw new BadRequestException("Column parameter not specified");
        if (params.getValue() == null)
            throw new BadRequestException("Value parameter not specified");


        //column parameter validation
        Step source = sourceSteps[0];
        for (DSDColumn column : source.getDsd().getColumns()) {
            if (params.getColumn().getId().equals(column.getId()))
                throw new BadRequestException("id equals");
            if (params.getColumn().getSubject() != null && column.getSubject() != null && params.getColumn().getSubject().equals(column.getSubject()))
                throw new BadRequestException("subject equals");
        }
        // TODO validate params with source

        Object value = params.getValue();
        if (value == null)
            throw new BadRequestException("id equals");

        if (value instanceof AddColumnMap) {
            AddColumnMap firstParameters = (AddColumnMap) value;
            if (firstParameters.getKeys().length != firstParameters.getValues().length)
                throw new BadRequestException("Inside of value parameters, key parameters's number should follows value parameter's one");
            for (Object keyParameter : firstParameters.getKeys()) {

                if (keyParameter == null)
                    throw new BadRequestException("key parameter should not be null");

                if (keyParameter instanceof AddColumnMap) {
                    AddColumnMap secondParameters = (AddColumnMap) keyParameter;
                    for (Object secondKey : secondParameters.getKeys()) {
                        if (secondKey != null) {
                            if (!(secondKey instanceof String))
                                throw new BadRequestException("second level key parameter can be only an id");
                            if (source.getDsd().findColumn(secondKey.toString()) == null) {
                                throw new BadRequestException("second level key parameter id is not in the resource specified");
                            }
                        }
                    }
                }

            }
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




