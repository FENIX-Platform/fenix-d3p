package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.type.ProcessName;

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

    @Override
    public Step process(Object params, Step... sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || type!=StepType.table)
            throw new UnsupportedOperationException("distinct process support only one table input step");
        String tableName = (String) source.getData();
        DSDDataset dsd = source.getDsd();
        //Update columns distinct values
        Connection connection = source.getStorage().getConnection();
        try {
            for (DSDColumn column : dsd.getColumns())
                updateColumnDistinct(connection, tableName, column);
        } finally {
            connection.close();
        }
        //Build resulting step
        TableStep result = (TableStep)stepFactory.getInstance(StepType.table);
        result.setData(tableName);
        result.setDsd(dsd);
        return result;
    }

    private void updateColumnDistinct(Connection connection, String tableName, DSDColumn column) throws SQLException {
        DSDDomain distinct = column!=null && !"value".equals(column.getSubject()) ? column.getValues() : null;
        if (distinct!=null) {
            switch (column.getDataType()) {
                case code:
                case customCode:
                    Collection<OjCodeList> codelists = distinct.getCodes();
                    OjCodeList codelist = codelists!=null && codelists.size()>0 ? codelists.iterator().next() : null;
                    Collection<OjCode> codes = codelist!=null ? codelist.getCodes() : null;

                    if (codes!=null && codes.size()>0) {
                        Set rawValues = selectColumnDistinct(connection, tableName, column.getId());
                        Collection<OjCode> values = new LinkedList<>();
                        for (OjCode code : codes)
                            if (rawValues.contains(code.getCode()))
                                values.add(code);
                        codelist.setCodes(values);
                    }
                    break;
                case date:
                case month:
                case year:
                case time:
                    Collection<Long> times = distinct.getTimeList();
                    if (times!=null && times.size()>0) {
                        times = new LinkedList<>(times);
                        times.retainAll(selectColumnDistinct(connection, tableName, column.getId()));
                        distinct.setTimeList(times);
                    }
                    break;
                case enumeration:
                case text:
                    Collection<String> enumeration = distinct.getEnumeration();
                    if (enumeration!=null && enumeration.size()>0) {
                        enumeration = new LinkedList<>(enumeration);
                        enumeration.retainAll(selectColumnDistinct(connection, tableName, column.getId()));
                        distinct.setEnumeration(enumeration);
                    }
                    break;
                default:
                    column.setValues(null);
            }
        }
    }

    private Set selectColumnDistinct(Connection connection, String tableName, String id) throws SQLException {
        Set data = new HashSet();
        Object value;
        for (ResultSet rawData = connection.createStatement().executeQuery("SELECT DISTINCT ("+id+") FROM "+tableName); rawData.next(); )
            if ((value = rawData.getObject(1))!=null)
                data.add(value);
        return data;
    }


}
