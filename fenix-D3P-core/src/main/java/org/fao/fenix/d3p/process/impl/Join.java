package org.fao.fenix.d3p.process.impl;

import org.apache.log4j.Logger;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;
import org.fao.fenix.d3p.process.impl.join.JoinLogicFactory;
import org.fao.fenix.d3p.process.type.ProcessName;
import javax.ws.rs.BadRequestException;

@ProcessName("join")
public class Join extends org.fao.fenix.d3p.process.Process<JoinParams>{

    private static final Logger LOGGER = Logger.getLogger(Join.class);
    private JoinLogic joinLogic;

    @Override
    public Step process(JoinParams params, Step... sourceStep) throws Exception {
        LOGGER.info("Start join process");

        initialValidation(sourceStep);

        // factory should create the appropriate logic depending on
        // the parameters configuration

        joinLogic = new JoinLogicFactory().getInstance(params);
        joinLogic.process(sourceStep);


        return null;
    }


    private void initialValidation (Step... sourceSteps) {

        if(sourceSteps== null || sourceSteps.length <2){
            throw new BadRequestException("join should have at least two sid");
        }
        for(Step step: sourceSteps){
            StepType type= step.getType();
            if (type==null || (type != StepType.table ))
                throw new UnsupportedOperationException("filter process can be applied only on a table or an other select query");
        }
    }

}
