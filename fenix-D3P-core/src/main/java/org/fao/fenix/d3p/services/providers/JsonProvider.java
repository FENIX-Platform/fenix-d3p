package org.fao.fenix.d3p.services.providers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.fao.fenix.commons.process.dto.Process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;


public abstract class JsonProvider {
    protected ObjectMapper jacksonMapper = new ObjectMapper();

    protected JsonNode getRoot (String source) throws IOException {
        //ArrayNode
        return source!=null ? jacksonMapper.readTree(source) : null;
    }


    //Utils
    protected String readSection(BufferedReader in, String placeHolder) throws IOException {
        StringBuilder buffer = new StringBuilder();
        for (String row=in.readLine(); row!=null && !row.trim().equals(placeHolder); row=in.readLine())
            buffer.append(row).append('\n');
        //Remove BOM character if it exists
        return buffer.length()>0 && buffer.charAt(0) == 65279 ? buffer.substring(1) : buffer.toString();
    }

    protected String readContent(InputStream inputStream) {
        try (Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name()).useDelimiter("\\A")) {
            return scanner.hasNext() ? scanner.next() : "";
        }
    }

/*
    //Utils
    protected Resource decodeResource(String source, RepresentationType resourceType) throws Exception {
        switch (resourceType) {
            case codelist:  return JSONUtils.decode(source, Resource.class, DSDCodelist.class, Code.class);
            case dataset:   return JSONUtils.decode(source, Resource.class, DSDDataset.class, Object[].class);
            case geographic:return JSONUtils.decode(source, Resource.class, DSDGeographic.class, Object.class);
            case document:  return JSONUtils.decode(source, Resource.class, DSDDocument.class, Object.class);
            default: return null;
        }
    }
    protected <T> Collection<T> decodeData(String source, RepresentationType resourceType) throws Exception {
        Class<?> type;
        switch (resourceType) {
            case codelist:      type = Code.class; break;
            case dataset:       type = Object[].class; break;
            case geographic:    type = Object.class; break;
            case document:      type = Object.class; break;
            default: return null;
        }
        return JSONUtils.decode(source, Collection.class, type);
    }
    protected <T extends DSD> MeIdentification<T> decodeMetadata(String source, RepresentationType resourceType) throws Exception {
        Class<? extends DSD> type;
        switch (resourceType) {
            case codelist:      type = DSDCodelist.class; break;
            case dataset:       type = DSDDataset.class; break;
            case geographic:    type = DSDGeographic.class; break;
            case document:      type = DSDDocument.class; break;
            default: return null;
        }
        return JSONUtils.decode(source, MeIdentification.class, type);
    }

    protected RepresentationType getRepresentationType(String source, String metadataField) throws Exception {
        JsonNode metadataNode = source!=null ? jacksonMapper.readTree(source) : null;
        return getRepresentationType(metadataNode, metadataField);
    }
    protected RepresentationType getRepresentationType(JsonNode metadataNode, String metadataField) throws Exception {
        if (metadataField!=null && metadataNode!=null)
            for (String field : metadataField.split("\\."))
                metadataNode = metadataNode.get(field);

        String representationTypeLabel = metadataNode!=null ? metadataNode.path("meContent").path("resourceRepresentationType").textValue() : null;

        if (representationTypeLabel==null) {
            String rid = metadataNode!=null ? metadataNode.path("rid").textValue() : null;
            String uid = metadataNode!=null ? metadataNode.path("uid").textValue() : null;
            String version = metadataNode!=null ? metadataNode.path("version").textValue() : null;

            ODatabaseDocument connection = DatabaseStandards.connection.get().getUnderlying();
            ODocument metadataO = null;
            if (rid!=null)
                metadataO = connection.load(JSONEntity.toRID(rid));
            else if (uid!=null) {
                List<ODocument> metadataOList = connection.query(new OSQLSynchQuery<MeIdentification>("select from MeIdentification where index|id = ?"), uid + (version != null ? '|'+version : ""));
                metadataO = metadataOList!=null && !metadataOList.isEmpty() ? metadataOList.iterator().next() : null;
            }

            representationTypeLabel = metadataO!=null ? (String)metadataO.field("meContent.resourceRepresentationType") : null;
        }

        return representationTypeLabel!=null ? RepresentationType.valueOf(representationTypeLabel) : RepresentationType.dataset;
    }
*/
}
