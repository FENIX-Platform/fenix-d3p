package org.fao.fenix.d3p.process.impl.join.logic;


import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;

public class AutomaticJoin implements JoinLogic{


    @Override
    public Step process(Step[] sourceStep, DSDDataset[] dsdList, JoinParams params) throws Exception {
        return null;
    }

    @Override
    public void validate(Step[] sourceStep, DSDDataset[] dsdList, JoinParams params) throws Exception {

    }
}
