package org.fao.fenix.d3p.process;



import org.fao.fenix.commons.utils.JSONUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

public abstract class Process <T> {

    public Type getParametersType() {
        return ((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }


    public static void main(String[] args) throws Exception {
        Process<Map<String,Integer>> process = new Process<Map<String, Integer>>() {
            @Override
            public void init(Connection connection) {

            }

            @Override
            public String process(String source, Map<String, Integer> params) {
                return null;
            }
        };
        org.fao.fenix.commons.process.dto.Process processBean = JSONUtils.decode("{   \"name\" : \"test\",   \"parameters\" : {     \"a\" : 10,     \"b\" : \"2a\"   } }", org.fao.fenix.commons.process.dto.Process.class, process.getParametersType());
        System.out.println(processBean);
        org.fao.fenix.commons.process.dto.Process processBean2 = JSONUtils.decode("{   \"name\" : \"test\",   \"parameters\" : {     \"a\" : 10,     \"b\" : 2   } }", org.fao.fenix.commons.process.dto.Process.class, Map.class);
        System.out.println(processBean);
    }

    public abstract void init(Connection connection);

    public abstract String process(String source, T params);
/*
    public String process(String source, Map<String,Object> params) {
        T decodedParams = null;
        //TODO decode params

        return process(source, decodedParams);
    } */
}
