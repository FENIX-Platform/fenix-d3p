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

}
