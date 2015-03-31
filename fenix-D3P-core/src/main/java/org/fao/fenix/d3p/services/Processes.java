package org.fao.fenix.d3p.services;

import org.fao.fenix.commons.msd.dto.data.ResourceProxy;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.commons.process.dto.Process;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("/processes")
@Produces(MediaType.APPLICATION_JSON+"; charset=utf-8")
@Consumes(MediaType.APPLICATION_JSON)
public class Processes {
    private @Context HttpServletRequest httpRequest;

    /**
     * Apply a process workflow to a domain or a cached resource.
     * To store processes results to be consumed a second level cache is needed.
     * No pagination will be available
     * @param uid Source resource uid
     * @param flow Processes ids and parameters
     * @return Resource data
     */
    @POST
    @Path("{uid}")
    public ResourceProxy apply(@PathParam("uid") String uid, Process[] flow) {
        return apply(uid,null,flow);
    }

    /**
     * Apply a process workflow to a domain or a cached resource.
     * To store processes results to be consumed a second level cache is needed.
     * No pagination will be available
     * @param uid Source resource uid
     * @param version Source resource version
     * @param flow Processes ids and parameters
     * @return Resource data
     */
    @POST
    @Path("{uid}/{version}")
    public ResourceProxy apply(@PathParam("uid") String uid, @PathParam("version") String version, Process[] flow) {
        return null;
    }


}
