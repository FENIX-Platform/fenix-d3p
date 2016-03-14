package org.fao.fenix.d3p.process.impl.union;

import org.fao.fenix.d3p.process.type.UnionLogicName;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Iterator;

public class LogicFactory {

    @Inject private Instance<Logic> instances;

    public Logic getInstance (String name) {
        if (name!=null)
            for (Iterator<Logic> logicIterator = instances.iterator(); logicIterator.hasNext();) {
                Logic logic = logicIterator.next();
                if (logic.getClass().getAnnotation(UnionLogicName.class).value().equals(name))
                    return logic;
            }
        return null;
    }

}
