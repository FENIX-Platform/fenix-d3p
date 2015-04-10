package org.fao.fenix.d3p.process;



import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.Map;

public abstract class Process <T> {

    public Class[] getType() {
        Type[] types = ((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments();
        Class[] classes = new Class[types.length];
        for (int i=0; i<types.length; i++)
            classes[i] = types[i].getClass();
        return null;
    }

    public abstract void init(Connection connection);

    public abstract String process(String source, T params);

    public String process(String source, Map<String,Object> params) {
        T decodedParams = null;
        //TODO decode params

        return process(source, decodedParams);
    }
}
