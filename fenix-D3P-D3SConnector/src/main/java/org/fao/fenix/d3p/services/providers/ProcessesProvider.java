package org.fao.fenix.d3p.services.providers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.fao.fenix.commons.process.dto.Process;
import org.fao.fenix.commons.utils.JSONUtils;
import org.fao.fenix.commons.utils.Order;
import org.fao.fenix.commons.utils.Page;
import org.fao.fenix.d3p.process.ProcessFactory;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.LinkedList;

@Provider
@Consumes(MediaType.APPLICATION_JSON)
public class ProcessesProvider extends JsonProvider implements MessageBodyReader<Process[]> {
    @Inject ProcessFactory processFactory;
    @Inject DatabaseStandards additionalParameters;

    @Override
    public boolean isReadable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return true;
    }

    @Override
    public Process[] readFrom(Class<Process[]> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> multivaluedMap, InputStream inputStream) throws IOException, WebApplicationException {
        try {
            Collection<Process> processesInfo = decodeProcesses((ArrayNode)getRoot(readContent(inputStream)));
            //Add ordering
            Order orderingParameters = additionalParameters.getOrderingInfo();
            if (orderingParameters!=null && orderingParameters.isInitialized())
                processesInfo.add(new Process("order", orderingParameters));
            //Add pagination
            Page pageParameters = additionalParameters.getPaginationInfo();
            if (pageParameters!=null && pageParameters.isInitialized())
                processesInfo.add(new Process("page", pageParameters));
            //Return processes
            return processesInfo.toArray(new Process[processesInfo.size()]);
        } catch (Exception e) {
            throw new WebApplicationException(e);
        }
    }

    private Collection<Process> decodeProcesses(ArrayNode sources) throws Exception {
        Collection<Process> processesData = new LinkedList<>();
        for (JsonNode source : sources)
            processesData.add(decodeProcess(source));
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
            throw new BadRequestException("Requested process '"+name+"' not found");
    }

}
