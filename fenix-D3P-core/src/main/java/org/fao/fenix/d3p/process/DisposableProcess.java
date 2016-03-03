package org.fao.fenix.d3p.process;

import java.sql.Connection;

public abstract class DisposableProcess<T> extends Process<T> {

    public abstract void dispose() throws Exception;

}
