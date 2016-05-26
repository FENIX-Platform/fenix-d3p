package org.fao.fenix.d3p.process.impl;

import org.apache.log4j.Logger;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;
import org.fao.fenix.d3p.process.impl.join.JoinLogicFactory;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;

@ProcessName("join")
public class Join extends org.fao.fenix.d3p.process.Process<JoinParams>{

    private static final Logger LOGGER = Logger.getLogger(Join.class);
    @Inject JoinLogicFactory logicFactory;

    @Override
    public Step process(JoinParams params, Step... sourceStep) throws Exception {
        LOGGER.info("Start join process");

        //Retrieve join logic manager
        JoinLogic joinLogic = logicFactory.getInstance(params.getLogic());
        if(joinLogic== null)
            throw new BadRequestException("specified join logic not found");

        //Create dsd list
        DSDDataset[] dsdList = new DSDDataset[sourceStep.length];
        for (int i=0; i<sourceStep.length; i++)
            dsdList[i] = sourceStep[i].getDsd();

        //Validation
        initialValidation(sourceStep);
        joinLogic.validate(sourceStep, dsdList, params);

        //Process
        return joinLogic.process(sourceStep, dsdList, params);
    }


    private void initialValidation (Step... sourceSteps) {

        if(sourceSteps== null || sourceSteps.length <2)
            throw new BadRequestException("join should have at least two sid");
        DatasetStorage storage = sourceSteps[0].getStorage();

        for(Step step: sourceSteps){
            StepType type= step.getType();
            if (type==null || ((type != StepType.table ) && ((type != StepType.query) )))
                throw new UnsupportedOperationException("join process can only be applied on a table or on other select query");
            if (!step.getStorage().equals(storage))
                throw new UnsupportedOperationException("join process can be applied only on dataset deployed on the same storage");

        }
    }

}
