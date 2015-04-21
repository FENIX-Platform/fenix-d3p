package org.fao.fenix.d3p.process;

import java.sql.Connection;

public abstract class CachedProcess extends Process {

    public abstract void dispose(Connection connection);

}
