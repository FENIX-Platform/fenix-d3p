package org.fao.fenix.d3p.process;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;


@ApplicationScoped
public class ProcessFactory {
    @Inject Instance<Process> instances;

    public void init(String basePackage) {

    }

    public Process getInstance(String alias) {
        return null;
    }
}
