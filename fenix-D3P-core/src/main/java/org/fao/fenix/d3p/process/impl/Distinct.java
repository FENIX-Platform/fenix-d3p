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

@ProcessName("distinct")
public class Distinct extends org.fao.fenix.d3p.process.Process {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;

    @Override
    public Step process(Connection connection, Object params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        if (source==null)
            return null;
        if (source.getType()==StepType.table) {
            String tableName = getCacheStorage().getTableName((String) source.getData());
            DSDDataset dsd = source.getDsd();
            Collection<DSDColumn> columns = dsd!=null ? dsd.getColumns() : null;
            if (columns==null)
                throw new Exception ("'distinct' process source data has no columns");

            for (DSDColumn column : columns)
                updateColumnDistinct(connection, tableName, column);
        } else
            throw new UnsupportedOperationException("Distinct values can be restored only from cached data");

        return source;
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
