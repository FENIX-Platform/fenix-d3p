package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.commons.utils.StringUtils;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.inject.Inject;
import javax.xml.bind.ValidationEvent;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@ProcessName("dsdDistinct")
public class Distinct extends org.fao.fenix.d3p.process.Process {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;
    private @Inject Resources resourcesService;

    @Override
    public Step process(Object params, Step... sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length>1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || type!=StepType.table)
            throw new UnsupportedOperationException("distinct process support only one table input step");
        String tableName = (String) source.getData();
        DSDDataset dsd = source.getDsd();
        //Update columns distinct values
        Connection connection = source.getStorage().getConnection();
        try {
            Map<String,Code> codesMap = getCodesMap(getCodeLists(dsd.getColumns()));
            for (DSDColumn column : dsd.getColumns())
                updateColumnDistinct(connection, tableName, column, codesMap);
        } finally {
            connection.close();
        }
        //Build resulting step
        TableStep result = (TableStep)stepFactory.getInstance(StepType.table);
        result.setData(tableName);
        result.setDsd(dsd);
        return result;
    }

    private void updateColumnDistinct(Connection connection, String tableName, DSDColumn column, Map<String,Code> codesMap) throws SQLException {
        DSDDomain distinct = new DSDDomain();
        if (!"value".equals(column.getSubject()))
            switch (column.getDataType()) {
                case customCode:
                    Map<String, OjCode> domainCodes = new HashMap<>();
                    for (OjCode code : column.getDomain().getCodes().iterator().next().getCodes())
                        domainCodes.put(code.getCode(), code);
                    distinct.setCodes(new LinkedList<OjCodeList>());
                    OjCodeList customCodelist = column.getDomain().getCodes().iterator().next().clone();
                    distinct.getCodes().add(customCodelist);

                    Collection<OjCode> valuesCustomCodes = new LinkedList<>();
                    for (Object value : selectColumnDistinct(connection, tableName, column.getId())) {
                        OjCode code = domainCodes.get(value);
                        valuesCustomCodes.add(code!=null ? code : toOjCode((String)value));
                    }
                    customCodelist.setCodes(valuesCustomCodes);
                    break;
                case code:
                    distinct.setCodes(new LinkedList<OjCodeList>());
                    OjCodeList codelist = column.getDomain().getCodes().iterator().next().clone();
                    distinct.getCodes().add(codelist);
                    String prefix = getId(codelist.getIdCodeList(), codelist.getVersion())+'_';

                    Collection<OjCode> valuesCodes = new LinkedList<>();
                    for (Object value : selectColumnDistinct(connection, tableName, column.getId())) {
                        Code code = codesMap.get(prefix+value);
                        valuesCodes.add(code!=null ? toOjCode(code) : toOjCode((String)value));
                    }
                    codelist.setCodes(valuesCodes);
                    break;
                case date:
                case month:
                case year:
                case time:
                    distinct.setTimeList(selectColumnDistinct(connection, tableName, column.getId()));
                    break;
                case enumeration:
                case text:
                    distinct.setEnumeration(selectColumnDistinct(connection, tableName, column.getId()));
                    break;
                default:
                    distinct = null;
            }
        else
            distinct = null;

        column.setValues(distinct);
    }

    private Collection selectColumnDistinct(Connection connection, String tableName, String id) throws SQLException {
        Collection data = new LinkedList();
        Object value;
        for (ResultSet rawData = connection.createStatement().executeQuery("SELECT DISTINCT ("+id+") FROM "+tableName+" ORDER BY "+id); rawData.next(); )
            if ((value = rawData.getObject(1))!=null)
                data.add(value);
        return data;
    }


    private OjCode toOjCode(String code) {
        if (code!=null) {
            OjCode ojCode = new OjCode();
            ojCode.setCode(code);
            return ojCode;
        } else
            return null;
    }
    private OjCode toOjCode(Code code) {
        if (code!=null) {
            OjCode ojCode = new OjCode();
            ojCode.setCode(code.getCode());
            ojCode.setLinkedCode(code);
            return ojCode;
        } else
            return null;
    }

    private Map<String,Code> getCodesMap(Collection<Resource<DSDCodelist, Code>> codelists) {
        Map<String,Code> codesMap = new HashMap<>();
        if (codelists!=null)
            for (Resource<DSDCodelist, Code> codelist : codelists)
                fillCodesMap(getId(codelist.getMetadata().getUid(), codelist.getMetadata().getVersion())+'_', codelist.getData(), codesMap);
        return codesMap;
    }
    private void fillCodesMap (String prefix, Collection<Code> codes, Map<String,Code> codesMap) {
        if (codes!=null)
            for (Code code : codes) {
                codesMap.put(prefix + code.getCode(), code);
                fillCodesMap(prefix, code.getChildren(), codesMap);
            }
    }

    private Collection<Resource<DSDCodelist, Code>> getCodeLists(Collection<DSDColumn> columns) throws Exception {
        Set<String> existingCodelists = new HashSet<>();
        Collection<Resource<DSDCodelist, Code>> codeLists = new LinkedList<>();
        for (DSDColumn column : columns)
            if (column.getDataType()==DataType.code) {
                OjCodeList domain = column.getDomain().getCodes().iterator().next();
                Resource<DSDCodelist, Code> codelist = existingCodelists.add(getId(domain.getIdCodeList(), domain.getVersion())) ? resourcesService.loadResource(domain.getIdCodeList(), domain.getVersion()) : null;
                if (codelist!=null)
                    codeLists.add(codelist);
            }
        return codeLists;
    }
    private String getId(String uid, String version) {
        return uid!=null ? (version!=null ? uid+'_'+version : uid) : "";
    }


}
