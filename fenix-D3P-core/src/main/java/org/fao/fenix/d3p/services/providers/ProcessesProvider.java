package org.fao.fenix.d3p.services.providers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
        try {
            return decodeProcesses((ArrayNode)getRoot(readContent(inputStream)));
        } catch (Exception e) {
            throw new WebApplicationException(e);
        }
    }

    private Process[] decodeProcesses(ArrayNode sources) throws Exception {
        int size = sources!=null ? sources.size() : 0;
        Process[] processesData = new Process[size];

        for (int i=0; i<size; i++)
            processesData[i] = decodeProcess(sources.get(i));

        return processesData;
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
