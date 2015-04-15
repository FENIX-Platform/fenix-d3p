package org.fao.fenix.d3p.services.providers;

import com.fasterxml.jackson.databind.JsonNode;
import org.fao.fenix.commons.process.dto.Process;
import org.fao.fenix.commons.utils.JSONUtils;
import org.fao.fenix.d3p.process.ProcessFactory;

import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

public class ProcessesProvider extends JsonProvider implements MessageBodyReader<Process[]> {
    @Inject ProcessFactory processFactory;

    @Override
    public boolean isReadable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return true;
    }

    @Override
    public Process[] readFrom(Class<Process[]> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> multivaluedMap, InputStream inputStream) throws IOException, WebApplicationException {
        JsonNode flowNodes = getRoot(readContent(inputStream));
        //TODO
        return new Process[0];
    }

    private Process decodeProcess(JsonNode source) throws Exception {
        JsonNode nameNode = source!=null ? source.get("name") : null;
        String name = nameNode!=null ? nameNode.textValue() : null;
        org.fao.fenix.d3p.process.Process processor = name!=null ? processFactory.getInstance(name) : null;

        if (processor!=null) {
            Type paramsType = processor.getParametersType();
            return paramsType==null ? JSONUtils.decode(source.toString(), Process.class) : JSONUtils.decode(source.toString(), Process.class, paramsType);
        } else
            return null;
    }

}
