package org.fao.fenix.d3p.services;

import org.fao.fenix.commons.process.dto.DomainFilter;
import org.fao.fenix.commons.process.dto.DomainStatus;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/domains")
@Produces(MediaType.APPLICATION_JSON+"; charset=utf-8")
@Consumes(MediaType.APPLICATION_JSON)
public class Domains {

    /**
     * Create a new domain from existing datasets and domains. For each new succesfull created domain will be created a correspondent DSDDataset metadata (with no MeIdentification).
     *
     * Into the cache database the filter md5 digest will be associated to the dsd rid and cached table to avoid to create multiple times the same domain.
     * Even the list of involved resources and correspondent last update date will be saved and evaluated on each request to this service for automatic existing data invalidation.
     *
     * Above digest will be used as domain uid and it can be used into the filter "uid" entry to include other domains into the source data.
     * If the filter contains one or more reference to unavailable domains an error will be thrown.
     * Execution is asynchronous.
     * During thec
     * @param filter Involved resources filter. The filter include a resources filter, a data filter to apply on each resource, a process flow to apply to retrieved data.
     * @param timeoutMillis Optional cached domain associated timeout (managed by cache level).
     * @return Domain status
     */
    @POST
    public DomainStatus create(DomainFilter filter, @QueryParam("timeout") @DefaultValue("-1") Long timeoutMillis) {
        return null;
    }

    /**
     * Get existing domain status.
     * @param uid Domain assigned uid
     * @return Domain status
     */
    @GET
    @Path("{uid}")
    public DomainStatus status(@PathParam("uid") String uid) {
        return null;
    }

    /**
     * Remove an existing domain.
     * @param uid Domain assigned uid
     * @return Previous domain status
     */
    @DELETE
    @Path("{uid}")
    public DomainStatus remove(@PathParam("uid") String uid) {
        return null;
    }

    /**
     * Force domain data refresh.
     * @param uid
     * @param timeoutMillis
     * @param synchronous
     * @return
     */
    @PUT
    @Path("{uid}")
    public DomainStatus refresh(@PathParam("uid") String uid, @QueryParam("timeout") @DefaultValue("-1") Long timeoutMillis, @QueryParam("sync") @DefaultValue("false") Boolean synchronous) {
        return null;
    }


}
