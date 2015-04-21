package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.DataFilter;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.type.Process;

import java.sql.Connection;
import java.util.Map;

@Process(name = "filter")
public class DefaultFilter extends org.fao.fenix.d3p.process.Process<DataFilter> {

    @Override
    public Step process(Connection connection, DataFilter params, Step... sourceStep) {
        return null;
    }
}
