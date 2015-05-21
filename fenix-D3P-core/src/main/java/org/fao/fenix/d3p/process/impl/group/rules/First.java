package org.fao.fenix.d3p.process.impl.group.rules;

import org.fao.fenix.d3p.process.ProcessFactory;
import org.fao.fenix.d3p.process.impl.group.Rule;
import org.fao.fenix.d3p.process.type.RuleName;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;

@RuleName("FIRST")
public class First extends Rule {
    private Object value;

    @Override
    public void init(Connection connection) throws SQLException {
        value = null;
    }

    @Override
    public int getType(int[] ints) throws SQLException {
        return java.sql.Types.DOUBLE;
    }

    @Override
    public void append(Object[] values) throws SQLException {
        if (value==null)
            value = values[0];
    }

    @Override
    public Object getResult() throws SQLException {
        return value;
    }
}
