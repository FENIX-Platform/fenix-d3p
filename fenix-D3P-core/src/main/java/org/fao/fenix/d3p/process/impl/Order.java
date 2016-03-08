package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.Collection;

@ProcessName("order")
public class Order extends org.fao.fenix.d3p.process.Process<org.fao.fenix.commons.utils.Order> {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;

    @Override
    public Step process(org.fao.fenix.commons.utils.Order params, Step... sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType sourceType = source!=null ? source.getType() : null;
        if (sourceType==null || (sourceType!=StepType.table && sourceType!=StepType.query))
            throw new UnsupportedOperationException("order process support only one table or query input step");
        String tableName = sourceType==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        //Create query
        String query = createOrderQuery(params, tableName);
        //Create query step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setData(query);
        if (sourceType==StepType.query) {
            step.setParams(((QueryStep) source).getParams());
            step.setTypes(((QueryStep) source).getTypes());
        }

        return step;
    }


    private String createOrderQuery(org.fao.fenix.commons.utils.Order ordering, String from) throws Exception {
        StringBuilder query = new StringBuilder("SELECT * FROM ").append(from);
        if (ordering!=null)
            query.append(ordering.toH2SQL());
        return query.toString();
    }

}
