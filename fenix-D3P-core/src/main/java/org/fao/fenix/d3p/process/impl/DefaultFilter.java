package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.*;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.d3p.dto.IteratorStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.TableStep;
import org.fao.fenix.d3p.process.dto.SimpleFilterParams;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Column;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.dto.dataset.Type;

import java.sql.Connection;
import java.sql.Types;
import java.util.*;

@ProcessName("simpleFilter")
public class DefaultFilter extends org.fao.fenix.d3p.process.Process<SimpleFilterParams> {

    @Override
    public Step process(Connection connection, SimpleFilterParams params, Step... sourceStep) throws Exception {
        TableStep source = sourceStep!=null && sourceStep.length==1 && sourceStep[0] instanceof TableStep ? (TableStep)sourceStep[0] : null;
        String tableName = source!=null ? source.getData() : null;
        DSDDataset dsd = source!=null ? source.getDsd() : null;
        if (tableName!=null && dsd!=null) {
            DataFilter filter = params!=null ? params.getFilter() : null;
            Order order = params!=null ? params.getOrder() : null;

            IteratorStep step = new IteratorStep();
            step.setDsd(filter(dsd, filter));
            step.setData(getCacheStorage().load(order,null,filter,new Table(source.getData(), source.getDsd())));
            return step;
        } else
            throw new Exception ("Source step for data filtering is unavailable or incomplete.");
    }

    private DSDDataset filter (DSDDataset source, DataFilter filter) {
        DSDDataset dsd = new DSDDataset();
        dsd.setAggregationRules(source.getAggregationRules());
        dsd.setContextSystem("D3P");

        Collection<String> columnsName = filter!=null ? filter.getColumns() : null;
        if (columnsName!=null && columnsName.size()>0) {
            Collection<DSDColumn> columns = new LinkedList<>();
            for (DSDColumn column : source.getColumns())
                if (columnsName.contains(column.getId()))
                    columns.add(column);
            dsd.setColumns(columns);
        } else
            dsd.setColumns(source.getColumns());
        //TODO filter distinct values based on rows filter (no cloning needed)
        return dsd;
    }

}
