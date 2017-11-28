package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.dto.UnionJoin;
import org.fao.fenix.d3p.process.dto.UnionParams;
import org.fao.fenix.d3p.process.impl.union.Logic;
import org.fao.fenix.d3p.process.impl.union.LogicFactory;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

@ProcessName("union")
public class Union extends org.fao.fenix.d3p.process.Process<UnionParams> {
    private @Inject LogicFactory logicFactory;
    private @Inject StepFactory stepFactory;


    @Override
    public Step process(UnionParams params, Step... sourceStep) throws Exception {
        //Check parameters and restireve logic
        if (sourceStep==null || sourceStep.length==0)
            throw new BadRequestException("filter union have no input");
        Logic unionLogic = logicFactory.getInstance(params!=null ? params.getLogic() : null);
        if (unionLogic==null)
            throw new BadRequestException("filter union have no logic with name '"+(params!=null ? params.getLogic() : null)+'\'');

        //Create union query steps grouped by storage
        QueryStep[] unionByStorage = unionLogic.getUnionQuerySteps(getStepByStorage(sourceStep, params.getJoin()));

        //Create a single result step
        if (unionByStorage.length>1) { //In case of multiple storage run query steps to retrieve iterators and create a unique IteratorStep
            //Retrieve data
            Collection<Object[]> resultingData = new LinkedList<>();
            for (QueryStep unionQueryStep : unionByStorage)
                resultingData.addAll(unionQueryStep.getResource(true).getData());
            //Create IteratorStep
            IteratorStep step = (IteratorStep)stepFactory.getInstance(StepType.iterator);
            step.setDsd(unionByStorage[0].getDsd());
            step.setData(resultingData.iterator());
            return step;
        } else { //In case of single storage simply return the resulting union QueryStep
            return unionByStorage[0];
        }
    }




    //Utils
    private Collection<Collection<Object[]>> getStepByStorage(Step[] sources, UnionJoin[] join) {
        if (join==null || join.length==0)
            join = new UnionJoin[sources.length];

        Map<String, Collection<Object[]>> sourcesByStorage = new HashMap<>();
        for (int i=0; i<sources.length; i++) {
            Collection<Object[]> storageSources = sourcesByStorage.get(sources[i].getStorage().getClass().getName());
            if (storageSources==null)
                sourcesByStorage.put(sources[i].getStorage().getClass().getName(), storageSources = new LinkedList<>());
            storageSources.add(new Object[]{sources[i],  join[i]});
        }
        return sourcesByStorage.values();
    }

}

