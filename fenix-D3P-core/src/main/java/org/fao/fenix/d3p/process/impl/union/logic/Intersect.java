package org.fao.fenix.d3p.process.impl.union.logic;

import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.UnionJoin;
import org.fao.fenix.d3p.process.impl.union.Logic;
import org.fao.fenix.d3p.process.type.UnionLogicName;

import javax.enterprise.context.ApplicationScoped;
import java.util.Collection;

@UnionLogicName("intersect")
@ApplicationScoped
public class Intersect implements Logic {


    @Override
    public QueryStep[] getUnionQuerySteps(Collection<Collection<Object[]>> sourcesByStorage, boolean labels) throws Exception {


        /*
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("filter union can be applied only on a table or query sources");


         */
        return new QueryStep[0];
    }


}
