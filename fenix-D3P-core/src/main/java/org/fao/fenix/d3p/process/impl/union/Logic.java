package org.fao.fenix.d3p.process.impl.union;

import org.fao.fenix.d3p.dto.QueryStep;

import java.util.Collection;
import java.util.Map;

public interface Logic {

    QueryStep[] getUnionQuerySteps(Collection<Collection<Object[]>> sourcesByStorage, boolean labels) throws Exception;

}
