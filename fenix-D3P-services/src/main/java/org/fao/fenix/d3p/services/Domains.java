package org.fao.fenix.d3p.services;

import org.fao.fenix.d3p.dto.DomainFilter;
import org.fao.fenix.d3p.dto.DomainStatus;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/domains")
@Produces(MediaType.APPLICATION_JSON+"; charset=utf-8")
@Consumes(MediaType.APPLICATION_JSON)
public class Domains {


    @POST
    public DomainStatus create(DomainFilter filter, @QueryParam("timeout") @DefaultValue("-1") Long timeoutMillis, @QueryParam("sync") @DefaultValue("false") Boolean synchronous) {
        return null;
    }

    @GET
    @Path("{uid}")
    public DomainStatus status(@PathParam("uid") String uid) {
        return null;
    }

    @DELETE
    @Path("{uid}")
    public DomainStatus remove(@PathParam("uid") String uid) {
        return null;
    }

    @PUT
    @Path("{uid}")
    public DomainStatus refresh(@PathParam("uid") String uid, @QueryParam("timeout") @DefaultValue("-1") Long timeoutMillis, @QueryParam("sync") @DefaultValue("false") Boolean synchronous) {
        return null;
    }


}
