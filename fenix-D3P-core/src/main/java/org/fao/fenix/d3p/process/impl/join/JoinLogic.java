package org.fao.fenix.d3p.process.impl.join;


import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.JoinParams;

public interface JoinLogic {


    Step process(Step[] sourceStep, DSDDataset[] dsdList, JoinParams params) throws Exception;
    void validate(Step[] sourceStep, DSDDataset[] dsdList, JoinParams params) throws Exception;



}



