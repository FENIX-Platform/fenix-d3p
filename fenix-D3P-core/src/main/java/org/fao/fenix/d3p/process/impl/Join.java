package org.fao.fenix.d3p.process.impl;

import org.apache.log4j.Logger;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.ws.rs.BadRequestException;
import java.util.Map;

@ProcessName("join")
public class Join extends org.fao.fenix.d3p.process.Process<JoinParams>{

    private static final Logger LOGGER = Logger.getLogger(Join.class);
    private int sourceSize;

    @Override
    public Step process(JoinParams params, Step... sourceStep) throws Exception {
        LOGGER.info("Start join process");

        initialValidation(sourceStep);

        // if exists parameters, manual type of join
        if(params!= null && params.getJoins()!= null && params.getJoins().size() >0) {

            this.sourceSize = sourceStep.length;

            parametersValidation(params);








        }
        // else automatic join
        else {

        }




        // first check of validation (sid should be at least two)

        // check if parameters exist

        // factory should create the appropriate logic depending on
        // the parameters configuration


        return null;
    }


    private void initialValidation (Step... sourceSteps) {

        if(sourceSteps== null || sourceSteps.length <2){
            throw new BadRequestException("join should have at least two sid");
        }

    }

    private void parametersValidation (JoinParams params) {
       if(params.getJoins()!= null && params.getJoins().size() ==sourceSize)
           return;
        throw new BadRequestException("joins num,ber parameters name should be the same of the sids");
    }

}
