package org.fao.fenix.d3p.process.impl;

import org.apache.log4j.Logger;
import org.fao.fenix.commons.msd.dto.data.ResourceProxy;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.OjCode;
import org.fao.fenix.commons.msd.dto.templates.codeList.Code;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.AddColumnParams;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;
import org.fao.fenix.d3s.msd.services.spi.Resources;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

@ProcessName("addcolumn")
public class AddColumn extends org.fao.fenix.d3p.process.Process<AddColumnParams> {

    private static final Logger LOGGER = Logger.getLogger(AddColumn.class);
    private @Inject StepFactory stepFactory;
    // to inject d3s services
    private @Inject Resources resource;
    Language[] languages;


    @Override
    public Step process(AddColumnParams params, Step... sourceStep) throws Exception {

        LOGGER.debug("start addColumn Process");
        validate(params, sourceStep);
        Step source = sourceStep[0];

        StepType type = source.getType();
        String tableName = type == StepType.table ? (String) source.getData() : '(' + (String) source.getData() + ") as " + source.getRid();

        DSDDataset dsd = source.getDsd();
        dsd.getColumns().add(params.getColumn());

        Map<String, Map<String, String>> labelCodes = new HashMap<>();


        //Add label with languages
        if (params.getColumn().getDataType() == DataType.code || params.getColumn().getDataType() == DataType.customCode) {
            languages = DatabaseStandards.getLanguageInfo();
            if (languages != null && languages.length > 0) {
                addLanguageColumnsToDSD(languages, dsd, params.getColumn());
                // get the codes
                ArrayList<Code> codes = (params.getColumn().getDataType() == DataType.customCode) ?
                        trasformToCode((ArrayList<OjCode>) params.getColumn().getDomain().getCodes().iterator().next().getCodes()) :
                        (ArrayList<Code>) (getCodelist(params.getColumn())).getData();
                // create the map of labels
                codesMap(codes, labelCodes);
            }
        }

        //Generate and return query step
        QueryStep step = (QueryStep) stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        dsd.setContextSystem("D3P");
        step.setData(buildQuery(params, tableName, source.getDsd().getColumns(), labelCodes));
        step.setTypes(type == StepType.query ? ((QueryStep) source).getTypes() : null);
        step.setParams(type == StepType.query ? ((QueryStep) source).getParams() : null);
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
/*
                validateValueOnDatatype(values.get(i), columnDatatype);
*/
            }
        } else if (value instanceof String || value instanceof Integer || value instanceof Boolean || value instanceof Double) {
            //check value type equal to the column datatype to be added
/*
            validateValueOnDatatype(value, columnDatatype);
*/

        } else
            throw new BadRequestException("value can be a map, a string an integer or a boolean");
    }

    private void initialValidation(Step... sourceSteps) {
        Step source = sourceSteps != null && sourceSteps.length > 0 ? sourceSteps[0] : null;
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
    private String buildQuery(AddColumnParams params, String tableName, Collection<DSDColumn> columns, Map<String, Map<String, String>> codesMap) {


        ArrayList<String> valuesList = new ArrayList<>();
        StringBuilder query = new StringBuilder("SELECT ");

        int offset = (languages == null || languages.length == 0) ? 1 : languages.length + 1;
        for (int i = 0; i < columns.size() - offset; i++)
            query.append(((List<DSDColumn>) columns).get(i).getId() + ",");

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
                        query.append(valuesFirstLevel.get(i) == null ? " ELSE NULL " : " ELSE " + buildRightString(valuesFirstLevel.get(i), params.getColumn().getDataType()));
                        // to create the label map
                        valuesList.add(valuesFirstLevel.get(i).toString());
                        break;
                    }

                    ArrayList<Object> keysSecondLevel = secondLevelMap.get("keys");
                    ArrayList<Object> valuesSecondLevel = secondLevelMap.get("values");
                    query.append(" WHEN ");

                    for (int j = 0, secondLevelSize = keysSecondLevel.size(); j < secondLevelSize; j++) {
                        query.append(keysSecondLevel.get(j).toString());

                        String valueFormatted = null;
                        if (valuesSecondLevel.get(j) != null)
                            valueFormatted = buildRightString(valuesSecondLevel.get(j), params.getColumn().getDataType());
                        String valueToAppend = valuesSecondLevel.get(j) == null ?
                                " IS NULL " :
                                " =" + valueFormatted;

                        // to create the label map
                        if (valuesSecondLevel.get(j) == null)
                            valuesList.add(valuesSecondLevel.get(j).toString());

                        query.append(valueToAppend);
                        query.append(" AND ");
                    }

                    // remove AND statement and add value
                    query.setLength(query.length() - 4);
                    String directValue = buildRightString(valuesFirstLevel.get(i), params.getColumn().getDataType());
                    // to create the label map
                    valuesList.add(valuesFirstLevel.get(i).toString());

                    query.append("THEN " + directValue);

                } else if (key instanceof String) {
                    query.append(" WHEN " + key);
                    query.append(" THEN ");
                    String directValue = buildRightString(valuesFirstLevel.get(i), params.getColumn().getDataType());

                    // to create the label map
                    valuesList.add(valuesFirstLevel.get(i).toString());

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
            String directValue = buildRightString(value, params.getColumn().getDataType());

            // to create the label map
            valuesList.add(value.toString());

            query.append(directValue + " END");
        } else {
            throw new UnsupportedOperationException("datatype not supported for value parameter");
        }
        query.append(" AS " + params.getColumn().getId());
        if (codesMap != null && codesMap.size() > 0)
            addLabelQuery(valuesList, codesMap, query, params.getColumn().getId());

        query.append(" FROM " + tableName + " ");
        return query.toString();
    }


    private void addLabelQuery(ArrayList<String> valuesList, Map<String, Map<String, String>> codesMap, StringBuilder query, String columnId) {

        String querySlice = query.substring(query.indexOf("CASE") - 1);

        for (Language language : languages) {
            query.append(", ");

            String labelCaseQuery = new String(querySlice);
            for (String value : valuesList) {
                Map<String, String> label = codesMap.get(value);
                if (label != null)
                    labelCaseQuery = labelCaseQuery.replace(value, label.get(language.getCode()));

            }
            labelCaseQuery = labelCaseQuery.replace(columnId, columnId + "_" + language.getCode());
            query.append(labelCaseQuery);
        }
    }

    // Utils
    private String buildRightString(Object value, DataType columnDatatype) {
        return (value instanceof String && (columnDatatype == DataType.code || columnDatatype == DataType.customCode || columnDatatype == DataType.text)) ? "\'" + value.toString() + "\'" : value.toString();
    }

    // no, beacause there could be an expression(string) to generate an integer
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

    private void addLanguageColumnsToDSD(Language[] languages, DSDDataset dsd, DSDColumn codeColumn) {

        for (Language language : languages) {
            if (dsd.findColumn(codeColumn.getId() + '_' + language.getCode()) == null) {
                DSDColumn column = codeColumn.clone();
                column.setId(codeColumn.getId() + '_' + language.getCode());
                column.setKey(false);
                column.setSubject(null);
                column.setDomain(null);
                column.setDataType(DataType.text);
                dsd.getColumns().add(column);
            }

        }
    }


    private ResourceProxy getCodelist(DSDColumn column) {

        try {
            return resource.getResourceByUID
                    (column.getDomain().getCodes().iterator().next().getIdCodeList(),
                            column.getDomain().getCodes().iterator().next().getVersion(), true, false, false, false);
        } catch (Exception ex) {
            throw new BadRequestException("this codelist: " + column.getDomain().getCodes().iterator().next().getIdCodeList() +
                    " cannot be found on the environment");
        }
    }


    // recursive function to create codes-label map
    private Map<String, Map<String, String>> codesMap(Collection<Code> codes, Map<String, Map<String, String>> labels) {
        if (codes != null)
            for (Code code : codes) {
                codesMap(code.getChildren(), labels);
                labels.put(code.getCode(), code.getTitle());
            }
        return labels;
    }


    private ArrayList<Code> trasformToCode(ArrayList<OjCode> codes) {
        ArrayList<Code> result = new ArrayList<>();
        for (OjCode customCode : codes)
            result.add(new Code(customCode.getCode(), customCode.getLabel()));
        return result;
    }

}




