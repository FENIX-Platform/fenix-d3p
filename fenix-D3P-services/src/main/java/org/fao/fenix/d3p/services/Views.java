package org.fao.fenix.d3p.services;

import org.fao.fenix.commons.msd.dto.data.dataset.Resource;
import org.fao.fenix.d3p.dto.DataFilter;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/views")
@Produces(MediaType.APPLICATION_JSON+"; charset=utf-8")
@Consumes(MediaType.APPLICATION_JSON)
public class Views {


    @POST
    @Path("/{uid}/{version}")
    public Resource create(@PathParam("uid") String uid, @PathParam("version") String version, DataFilter filter) {
        return null;
    }
    @POST
    @Path("/uid/{uid}")
    public Resource create(@PathParam("uid") String uid, DataFilter filter) {
        return null;
    }
    @POST
    @Path("/rid/{rid}")
    public Resource createByRid(@PathParam("rid") String rid, DataFilter filter) {
        return null;
    }
}
