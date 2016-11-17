package org.fao.fenix.d3p.process.dto;

import javax.enterprise.context.RequestScoped;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@RequestScoped
public class VariableStorage {

    private static ThreadLocal<Map<String,Object[]>> channelVariables = new ThreadLocal<>();
    private Map<String,Object[]> globalVariables = new HashMap<>();

    public Object getVariable(String name) {
        int index = -1;
        try {
            index = Integer.parseInt(name.substring(name.indexOf('[')+1, name.indexOf(']')));
            name = name.substring(0,name.indexOf('['));
        } catch (Exception ex) {}
        Object[] value = channelVariables.get()!=null && channelVariables.get().containsKey(name) ? channelVariables.get().get(name) : globalVariables.get(name);
        return index>=0 && value!=null ? value[index] : value;
    }
    public Object setChannelVariable(String name, Object[] value) {
        Map<String,Object[]> variables = channelVariables.get();
        if (variables==null)
            channelVariables.set(variables = new TreeMap<>());
        return variables.put(name, value);
    }
    public Object setGlobalVariable(String name, Object[] value) {
        return globalVariables.put(name, value);
    }


}
