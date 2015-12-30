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
        String tableName = source!=null ? (String)source.getData() : null;
        if (tableName==null)
            throw new Exception("No data to apply ordering");
        StepType sourceType = source.getType();
        if (sourceType==null || (sourceType!=StepType.table && sourceType!=StepType.query))
            throw new UnsupportedOperationException("Unsupported source step type. Only 'table' and 'query' step are supported");

        //Normalize table name
        tableName = sourceType==StepType.table ? tableName : '('+tableName+") as " + source.getRid();
        //Create and update query
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(source.getDsd());
        step.setData(createPageQuery(params, tableName));
        if (sourceType==StepType.query) {
            step.setParams(((QueryStep) source).getParams());
            step.setTypes(((QueryStep) source).getTypes());
        }
        step.setRid(getRandomTmpTableName());

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
