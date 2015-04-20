package org.fao.fenix.d3p.services;

import org.fao.fenix.commons.msd.dto.data.ResourceProxy;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.msd.dto.templates.export.combined.dataset.DSD;
import org.fao.fenix.commons.msd.dto.templates.export.combined.dataset.MetadataDSD;
import org.fao.fenix.commons.process.dto.Process;
import org.fao.fenix.d3p.dao.StorageFactory;
import org.fao.fenix.d3p.process.ProcessFactory;
import org.fao.fenix.d3s.cache.dto.StoreStatus;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("/processes")
@Produces(MediaType.APPLICATION_JSON+"; charset=utf-8")
@Consumes(MediaType.APPLICATION_JSON)
public class Processes {
    private @Context HttpServletRequest httpRequest;
    private @Inject StorageFactory storageFactory;
    private @Inject Resources resourcesService;
    private @Inject ProcessFactory processFactory;

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
    public ResourceProxy apply(@PathParam("uid") String uid, Process[] flow) throws Exception {
        return apply(uid,null,flow);
    }

    /**
     * Apply a process workflow to a domain or a cached resource.
     * To store processes results to be consumed a second level cache is needed.
     * No pagination will be available
     * The last process cannot be a cached process
     * @param uid Source resource uid
     * @param version Source resource version
     * @param flow Processes ids and parameters
     * @return Resource data
     */
    @POST
    @Path("{uid}/{version}")
    public ResourceProxy apply(@PathParam("uid") String uid, @PathParam("version") String version, Process[] flow) throws Exception {
        if (flow==null || flow.length==0)
            return resourcesService.getResourceByUID(uid,version,false,true,true,false);


        String id = getId(uid, version);
        DatasetStorage storage = storageFactory.getDatasetStorage();
        StoreStatus resourceStatus = id!=null && storage!=null ? storage.loadMetadata(id) : null;
        MeIdentification<DSDDataset> metadata = resourcesService.loadMetadata(uid, version);


        org.fao.fenix.d3p.process.Process[] processes = new org.fao.fenix.d3p.process.Process[flow.length];
        for (int i=0; i<flow.length; i++)
            processes[i] = processFactory.getInstance(flow[i].getName());
        //id and metadata are the inputs for the flow




        return null;
    }



    private String getId(String uid, String version) {
        if (uid!=null)
            return version!=null ? uid+'|'+version : uid;
        else
            return null;
    }

}
