package org.fao.fenix.d3p.process.impl.join;

import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.impl.join.logic.AutomaticJoin;
import org.fao.fenix.d3p.process.impl.join.logic.ManualJoin;

public class JoinLogicFactory {

    private JoinLogic instance;

    public JoinLogic getInstance (JoinParams params) {

        if(this.instance == null){
            this.instance = (params!=null)?new ManualJoin( params ) : new AutomaticJoin();
        }
        return this.instance;
    }
}
