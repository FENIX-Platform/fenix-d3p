package org.fao.fenix.d3p.process.impl.join;

import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.impl.join.logic.AutomaticJoin;
import org.fao.fenix.d3p.process.impl.join.logic.ManualJoin;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

@ApplicationScoped
public class JoinLogicFactory {

    private @Inject Instance<JoinLogic> instance;

    public JoinLogic getInstance (String logicName) {
        return instance.select(ManualJoin.class).iterator().next();
    }
}
