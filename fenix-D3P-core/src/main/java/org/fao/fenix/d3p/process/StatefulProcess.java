package org.fao.fenix.d3p.process;

import java.sql.Connection;

public abstract class StatefulProcess<T> extends Process<T> {

    public abstract void dispose(Connection connection) throws Exception;

}
