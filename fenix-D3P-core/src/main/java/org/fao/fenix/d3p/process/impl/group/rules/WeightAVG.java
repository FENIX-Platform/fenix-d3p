package org.fao.fenix.d3p.process.impl.group.rules;

import org.fao.fenix.d3p.process.ProcessFactory;
import org.fao.fenix.d3p.process.impl.group.Rule;
import org.fao.fenix.d3p.process.type.RuleName;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;

@RuleName("WAVG")
public class WeightAVG extends Rule {
    private int count;
    private double sum;

    private @Inject ProcessFactory factory;
    private ProcessFactory getFactory() {
        return factory;
    }

    @Override
    public void init(Connection connection) throws SQLException {
        count = 0;
        sum = 0;
    }

    @Override
    public int getType(int[] ints) throws SQLException {
        return java.sql.Types.DOUBLE;
    }

    @Override
    public void append(Object[] values) throws SQLException {
        //Test injection
        if (factory==null)
            factory = getSource(this).getFactory();

        Number value = (Number)values[0];
        Number weight = (Number)values[1];
        if (value!=null) {
            sum += value.doubleValue() * (weight!=null ? weight.doubleValue() : 1);
            count++;
        }
    }

    @Override
    public Object getResult() throws SQLException {
        return sum/count;
    }
}
