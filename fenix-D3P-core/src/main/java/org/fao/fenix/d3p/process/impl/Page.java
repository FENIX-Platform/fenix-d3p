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
    public Step process(Connection connection, org.fao.fenix.commons.utils.Page params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        if (source==null)
            return null;
        StepType sourceType = source.getType();
        if (sourceType==null || (sourceType!=StepType.table && sourceType!=StepType.query))
            throw new UnsupportedOperationException("Unsupported source step type. Only 'table' and 'query' step are supported");

        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(source.getDsd());
        if (source.getType()==StepType.table) {
            step.setData(createPageQuery(params, (String) source.getData()));
        } else {
            step.setData(createPageQuery(params, '(' + (String) source.getData() + ')'));
            step.setParams(((QueryStep)source).getParams());
            step.setTypes(((QueryStep)source).getTypes());
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
