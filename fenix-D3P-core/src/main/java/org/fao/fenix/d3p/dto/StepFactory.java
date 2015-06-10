package org.fao.fenix.d3p.dto;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;

public class StepFactory {
    @Inject Instance<Step> instance;

    public Step getInstance(StepType type) {
        if (type!=null)
            switch (type) {
                case iterator: return instance.select(IteratorStep.class).iterator().next();
                case query: return instance.select(QueryStep.class).iterator().next();
                case table: return instance.select(TableStep.class).iterator().next();
            }
        return null;
    }
}
