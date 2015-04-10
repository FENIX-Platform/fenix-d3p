package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.find.dto.filter.DataFilter;
import org.fao.fenix.d3p.process.type.Process;

import java.sql.Connection;
import java.util.Map;

@Process(name = "filter")
public class DefaultFilter extends org.fao.fenix.d3p.process.Process<DataFilter> {

    @Override
    public void init(Connection connection) {

    }

    @Override
    public String process(String source, DataFilter params) {
        return null;
    }

}
