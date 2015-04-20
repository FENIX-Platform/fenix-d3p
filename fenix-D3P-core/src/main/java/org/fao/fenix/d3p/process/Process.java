package org.fao.fenix.d3p.process;



import org.fao.fenix.d3p.dto.Step;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class Process <T> {

    public Type getParametersType() {
        return ((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * Execute the process. It's a synchronous activity.
     * @param sourceStep previous step
     * @param params
     * @return
     */
    public abstract Step process(T params, Step ... sourceStep);
/*
    public String process(String source, Map<String,Object> params) {
        T decodedParams = null;
        //TODO decode params

        return process(source, decodedParams);
    } */
}
