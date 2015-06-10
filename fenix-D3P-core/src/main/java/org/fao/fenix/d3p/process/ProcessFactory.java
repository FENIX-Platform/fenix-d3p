package org.fao.fenix.d3p.process;

import org.fao.fenix.d3p.process.type.ProcessName;
import org.reflections.Reflections;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


@ApplicationScoped
public class ProcessFactory {
    private @Inject Instance<Process> instances;
    private Map<String, Class<? extends Process>> processesClass = new HashMap<>();

    public void init(String basePackage) {
        Reflections reflections = new Reflections(basePackage);
        Set<Class<? extends Process>> subTypes = reflections.getSubTypesOf(Process.class);
        subTypes.addAll(reflections.getSubTypesOf(StatefulProcess.class));
        if (subTypes!=null)
            for (Class<? extends Process> processClass : subTypes) {
                //Retrieve name
                ProcessName annotation = processClass.getAnnotation(ProcessName.class);
                String name = annotation!=null ? annotation.value() : null;
                name = name==null ? processClass.getSimpleName() : name;
                //Maintain the class
                processesClass.put(name, processClass);
            }
    }

    public Process getInstance(String alias) {
        Class<? extends Process> processClass = processesClass.get(alias);
        return processClass!=null ? instances.select(processClass).iterator().next() : null;
    }
}
