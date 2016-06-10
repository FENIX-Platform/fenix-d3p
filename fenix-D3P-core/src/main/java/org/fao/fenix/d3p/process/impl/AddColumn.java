package org.fao.fenix.d3p.process.impl;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.log4j.Logger;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
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
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.xml.crypto.Data;
import java.util.*;

@ProcessName("addcolumn")
public class AddColumn extends org.fao.fenix.d3p.process.Process<AddColumnParams> {

    private static final Logger LOGGER = Logger.getLogger(AddColumn.class);
    private
    @Inject
    StepFactory stepFactory;

    @Override
    public Step process(AddColumnParams params, Step... sourceStep) throws Exception {

        LOGGER.debug("start addColumn Process");
        validate(params, sourceStep);
        Step source = sourceStep[0];

        StepType type = source.getType();
        String tableName = type == StepType.table ? (String) source.getData() : '(' + (String) source.getData() + ") as " + source.getRid();

        DSDDataset dsd = source.getDsd();
        dsd.getColumns().add(params.getColumn());

        //Generate and return query step
        QueryStep step = (QueryStep) stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        dsd.setContextSystem("D3P");
        step.setData(buildQuery(params, tableName, source.getDsd().getColumns()));
        step.setTypes(type==StepType.query ? ((QueryStep)source).getTypes() : null);
        step.setParams(type==StepType.query ? ((QueryStep)source).getParams() : null);
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
        DataType columnDatatype = params.getColumn().getDataType();
        Object value = params.getValue();
        if (value == null)
            throw new BadRequestException("id equals");

        if (value instanceof Map) {
            HashMap<String, ArrayList<Object>> valueMap = (HashMap<String, ArrayList<Object>>) value;
            if (valueMap == null || valueMap.get("keys") == null || valueMap.get("values") == null)
                throw new BadRequestException("Map configuration is not right");

            ArrayList<Object> keys = valueMap.get("keys");
            ArrayList<Object> values = valueMap.get("values");

            if (keys.size() < 1 || keys.size() != values.size())
                throw new BadRequestException("keys length should follow the value length");

            for (int i = 0, size = keys.size(); i < size; i++) {
                if (keys.get(i) == null)
                    throw new BadRequestException("Inside of value parameters, key parameters's number should follows value parameter's one");
                if (keys.get(i) instanceof Map) {
                    HashMap<String, ArrayList<Object>> valueSecondMap = (HashMap<String, ArrayList<Object>>) keys.get(i);

                    //if second map is not null and not empty
                    if (valueSecondMap != null && !valueSecondMap.isEmpty()) {

                        ArrayList<Object> secondKeys = valueSecondMap.get("keys");
                        ArrayList<Object> secondValues = valueSecondMap.get("values");
                        if (secondKeys.size() < 1 || secondKeys.size() != secondValues.size())
                            throw new BadRequestException("second keys length should follow the value length");

                        for (Object secondaryKey : secondKeys) {

                            if (!(secondaryKey instanceof String))
                                throw new BadRequestException("second keys can be only a string that represent a column id");
                            if (source.getDsd().findColumn(secondaryKey.toString()) == null)
                                throw new BadRequestException("second keys column id " + secondaryKey.toString() + " is not present into the dataset");
                        }

                    }
                }
                //check value type equal to the column datatype to be added
                validateValueOnDatatype(values.get(i), columnDatatype);
            }
        } else if (value instanceof String || value instanceof Integer || value instanceof Boolean || value instanceof Double) {
            //check value type equal to the column datatype to be added
            validateValueOnDatatype(value, columnDatatype);

        } else
            throw new BadRequestException("value can be a map, a string an integer or a boolean");
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

    // Logic
    private String buildQuery(AddColumnParams params, String tableName, Collection<DSDColumn> columns ) {


        StringBuilder query = new StringBuilder("SELECT ");

        for(int i=0; i< columns.size(); i++) {
            if(i!= columns.size()-1)
                query.append( ((ArrayList<DSDColumn>) columns).get(i).getId() + ",");
        }
        Object value = params.getValue();
        query.append(" CASE ");

        if (value instanceof Map) {
            // check the conditions
            HashMap<String, ArrayList<Object>> valueMap = (HashMap<String, ArrayList<Object>>) value;
            ArrayList<Object> keysFirstLevel = valueMap.get("keys");
            ArrayList<Object> valuesFirstLevel = valueMap.get("values");

            for (int i = 0; i < keysFirstLevel.size(); i++) {
                Object key = keysFirstLevel.get(i);

                // if second level map
                if (key instanceof Map) {
                    HashMap<String, ArrayList<Object>> secondLevelMap = (HashMap<String, ArrayList<Object>>) key;

                    // if key is null or empty, stop here
                    if (secondLevelMap == null || secondLevelMap.isEmpty()) {
                        query.append(valuesFirstLevel.get(i) == null? " ELSE NULL ":" ELSE "+ buildRightString(valuesFirstLevel.get(i)));
                        break;
                    }

                    ArrayList<Object> keysSecondLevel = secondLevelMap.get("keys");
                    ArrayList<Object> valuesSecondLevel = secondLevelMap.get("values");
                    query.append(" WHEN ");

                    for (int j = 0, secondLevelSize = keysSecondLevel.size(); j < secondLevelSize; j++) {
                        query.append(keysSecondLevel.get(j).toString());

                        String valueFormatted = null;
                        if (valuesSecondLevel.get(j) != null)
                            valueFormatted = buildRightString(valuesSecondLevel.get(j));
                        String valueToAppend = valuesSecondLevel.get(j) == null ?
                                " IS NULL " :
                                " =" + valueFormatted;
                        query.append(valueToAppend);
                        query.append(" AND ");
                    }

                    // remove AND statement and add value
                    query.setLength(query.length() - 4);
                    String directValue = buildRightString(valuesFirstLevel.get(i));
                    query.append("THEN " + directValue);

                } else if (key instanceof String) {
                    query.append(" WHEN " + key);
                    query.append(" THEN ");
                    String directValue = buildRightString(valuesFirstLevel.get(i));
                    query.append(directValue);

                } else if (key == null) {
                    query.append(" ELSE NULL");
                    break;
                } else {
                    throw new BadRequestException("error key");
                }
            }
            query.append(" END");
        } else if (value instanceof String || value instanceof Integer || value instanceof Boolean || value instanceof Double) {
            // direct values
            query.append("WHEN 1=1 THEN ");
            String directValue = buildRightString(value);
            query.append(directValue + " END");
        } else {
            throw new UnsupportedOperationException("datatype not supported for value parameter");
        }
        query.append(" AS " + params.getColumn().getId());

        query.append(" FROM "+ tableName+ " ");
        return query.toString();
    }

    // Utils
    private String buildRightString(Object value) {
        return (value instanceof String) ? "\'" + value.toString() + "\'" : value.toString();
    }

    private void validateValueOnDatatype(Object value, DataType columnDatatype) {
        switch (columnDatatype) {
            case code:
            case customCode:
            case text:
                if (!(value instanceof String))
                    throw new BadRequestException("this value : " + value.toString() + " does not follow the datatype of the column");
                break;
            // integer
            case time:
            case date:
            case month:
            case year:
                if (!(value instanceof Integer))
                    throw new BadRequestException("this value : " + value.toString() + " does not follow the datatype of the column");
                break;
            // double
            case number:
            case percentage:
                if (!(value instanceof Double))
                    throw new BadRequestException("this value : " + value.toString() + " does not follow the datatype of the column");
                break;
            case bool:
                if (!(value instanceof Boolean))
                    throw new BadRequestException("this value : " + value.toString() + " does not follow the datatype of the column");
                break;
        }
    }
}




