package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import java.sql.Connection;

@ProcessName("page")
public class Page extends org.fao.fenix.d3p.process.Process<org.fao.fenix.commons.utils.Page> {
    private @Inject StepFactory stepFactory;

    @Override
    public Step process(org.fao.fenix.commons.utils.Page params, Step... sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length>0 ? sourceStep[0] : null;
        StepType sourceType = source.getType();
        if (sourceType==null || (sourceType!=StepType.table && sourceType!=StepType.query))
            throw new UnsupportedOperationException("page process support only one table or query input step");
        String tableName = sourceType==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        //Create and update query
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setData(createPageQuery(params, tableName));
        if (sourceType==StepType.query) {
            step.setParams(((QueryStep) source).getParams());
            step.setTypes(((QueryStep) source).getTypes());
        }

        return step;
    }


    private String createPageQuery(org.fao.fenix.commons.utils.Page paging, String from) throws Exception {
        StringBuilder query = new StringBuilder("SELECT * FROM ").append(from);
        if (paging!=null) {
            paging.init();
            query.append(paging.toH2SQL());
        }
        return query.toString();
    }

}
