package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.ColumnsFilter;
import org.fao.fenix.commons.find.dto.filter.DataFilter;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

@ProcessName("columns")
public class Columns extends org.fao.fenix.d3p.process.Process<ColumnsFilter> {
    private @Inject StepFactory stepFactory;

    @Override
    public Step process(ColumnsFilter params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length>0 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("columns process can be applied only on a table or an other select query");
        String tableName = type==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();

        //Update dsd (add label columns if needed)
        Language[] languages = DatabaseStandards.getLanguageInfo();
        addLanguages(dsd, params, languages);
        updateDsd(dsd, params.getColumns(), languages);

        //Generate and return query step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(createQuery(dsd, tableName));
        step.setParams(type==StepType.query ? ((QueryStep)source).getParams() : new Object[0]);
        step.setTypes(type==StepType.query ? ((QueryStep)source).getTypes() : null);
        return step;
    }


    private void addLanguages (DSDDataset dsd, ColumnsFilter filter, Language[] languages) {
        Collection<String> columnsName = filter.getColumns();
        Collection<String> resultingColumnsName = new LinkedList<>();

        if (languages!=null && languages.length>0 && columnsName!=null && columnsName.size()>0) {
            for (String columnName : columnsName) {
                DSDColumn column = dsd.findColumn(columnName);
                if (column == null)
                    throw new BadRequestException("Column '" + columnName + "' not found during 'columns' step execution.");
                resultingColumnsName.add(columnName);
                if (column.getDataType() == DataType.code || column.getDataType() == DataType.customCode)
                    for (Language l : languages)
                        resultingColumnsName.add(columnName + '_' + l.getCode());
            }
            filter.setColumns(resultingColumnsName);
        }
    }

    private void updateDsd (DSDDataset dsd, Collection<String> columnsName, Language[] languages) {
        if (columnsName!=null && columnsName.size()>0) {
            if (languages!=null && languages.length>0)
                dsd.extend(languages);
            int keySize = 0;
            Collection<DSDColumn> columns = new LinkedList<>();
            for(String columnName: columnsName) {
                DSDColumn column = dsd.findColumn(columnName);
                columns.add(column);
                keySize += column.getKey()==Boolean.TRUE ? 1 : 0;
            }

            for (DSDColumn column : dsd.getColumns())
                keySize -= column.getKey()==Boolean.TRUE ? 1 : 0;
            if (keySize!=0)
                for (DSDColumn column : columns)
                    column.setKey(false);

            dsd.setColumns(columns);
        }
    }



    private String createQuery(DSDDataset dsd, String tableName) {
        StringBuilder columns = new StringBuilder();
        for (DSDColumn column : dsd.getColumns())
            columns.append(',').append(column.getId());
        return "SELECT "+columns.substring(1)+" FROM "+tableName;
    }



}

