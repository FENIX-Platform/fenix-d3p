package org.fao.fenix.d3p.services;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.data.ResourceProxy;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.msd.dto.templates.ResponseBeanFactory;
import org.fao.fenix.commons.msd.dto.templates.standard.combined.Metadata;
import org.fao.fenix.commons.process.dto.Process;
import org.fao.fenix.d3p.flow.FlowManager;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collection;

@Path("/processes")
@Produces(MediaType.APPLICATION_JSON+"; charset=utf-8")
@Consumes(MediaType.APPLICATION_JSON)
public class Processes {
    private static final String standardTemplatesBasePackage = Metadata.class.getPackage().getName();


    private @Inject Resources resourcesService;
    private @Inject FlowManager flowManager;


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
        //Retrieve source metadata
        MeIdentification<DSDDataset> metadata = resourcesService.loadMetadata(uid, version);
        if (metadata==null)
            return null;

        //Check flow availability
        if (flow==null || flow.length==0)
            return resourcesService.getResourceByUID(uid,version,false,true,true,false);

        //Prefetch source data
        resourcesService.fetch(metadata);

        //Apply flow
        Resource<DSDDataset,Object[]> result = flowManager.process(metadata, flow);
        Collection<Object[]> data = result!=null ? result.getData() : null;
        org.fao.fenix.commons.msd.dto.templates.standard.combined.dataset.DSD metadataProxy = result!=null ? ResponseBeanFactory.getInstance(result.getMetadata(), org.fao.fenix.commons.msd.dto.templates.standard.combined.dataset.DSD.class) : null;
        Long size = data!=null ? (long)data.size() : null;

        //Return proxy to the resulting data
        return new ResourceProxy(
                metadataProxy,
                data, null, null, size
        );
    }

}
