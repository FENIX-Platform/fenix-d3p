package org.fao.fenix.d3p.services;

import org.fao.fenix.commons.msd.dto.data.ResourceProxy;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.dto.Process;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.Map;

@Path("/processes")
@Produces(MediaType.APPLICATION_JSON+"; charset=utf-8")
@Consumes(MediaType.APPLICATION_JSON)
public class Processes {


    @POST
    @Path("{domain}")
    public ResourceProxy apply(@PathParam("domain") String domainUid, Process[] flow) {
        return null;
    }

    @GET
    @Path("{domain}/{process}")
    public ResourceProxy apply(@PathParam("domain") String domainUid, @PathParam("process") String processId) {
        return apply(domainUid, new Process[]{new ProcessStandard()});
    }


}
