package org.fao.fenix.d3p.domain.services;

import org.fao.fenix.d3p.domain.dto.DomainStatus;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/domains")
@Produces(MediaType.APPLICATION_JSON+"; charset=utf-8")
@Consumes(MediaType.APPLICATION_JSON)
public class Domains {

    @GET
    @Path("{id}")
    public DomainStatus status(@PathParam("id") String id) {
        return null;
    }
    @POST
    public DomainStatus create() {
        return null;
    }
    @DELETE
    @Path("{id}")
    public DomainStatus remove(@PathParam("id") String id) {
        return null;
    }



}
