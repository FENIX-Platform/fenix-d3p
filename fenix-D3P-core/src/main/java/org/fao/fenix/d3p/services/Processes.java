package org.fao.fenix.d3p.services;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.data.ResourceProxy;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.msd.dto.templates.ResponseBeanFactory;
import org.fao.fenix.commons.msd.dto.templates.standard.combined.Metadata;
import org.fao.fenix.commons.process.dto.Process;
import org.fao.fenix.commons.process.dto.StepId;
import org.fao.fenix.d3p.flow.FlowManager;
import org.fao.fenix.d3s.msd.services.spi.Resources;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Path("/processes")
@Produces(MediaType.APPLICATION_JSON+"; charset=utf-8")
@Consumes(MediaType.APPLICATION_JSON)
public class Processes {
    private static final String standardTemplatesBasePackage = Metadata.class.getPackage().getName();


    private @Inject DatabaseStandards parameters;
    private @Inject Resources resourcesService;
    private @Inject FlowManager flowManager;


    /**
     * Apply a process workflow to a domain or a cached resource.
     * No pagination will be available
     * @param uid Source resource uid
     * @param flow Processes ids and parameters
     * @param managerList Alternative comma separated flow managers list
     * @return Processed data
     */
    @POST
    @Path("{uid}")
    public ResourceProxy apply(@PathParam("uid") String uid, Process[] flow, @QueryParam("logic") String managerList) throws Exception {
        return apply(uid,null,flow, managerList);
    }

    /**
     * Apply a process workflow to a domain or a cached resource.
     * No pagination will be available
     * @param uid Source resource uid
     * @param version Source resource version
     * @param flow Processes ids and parameters
     * @param managerList Alternative comma separated flow managers list
     * @return Processed data
     */
    @POST
    @Path("{uid}/{version}")
    public ResourceProxy apply(@PathParam("uid") String uid, @PathParam("version") String version, Process[] flow, @QueryParam("logic") String managerList) throws Exception {
        //Overwrite source sid for the first process
        if (flow!=null && flow.length>0)
            flow[0].setSid(new StepId[]{new StepId(uid,version)});
        //If no flow is provided return original dataset
        else
            return resourcesService.getResourceByUID(uid,version,false,true,true,false);

        //Apply flow
        Map<StepId, ResourceProxy> results = apply(flow, managerList);
        if (results.size()>1)
            throw new BadRequestException("This entry point is only for single chain flow.");
        return results.size()>0 ? results.values().iterator().next() : null;
    }

    /**
     * Apply a process workflow.
     * No pagination will be available
     * @param flow Processes ids and parameters
     * @param managerList Alternative comma separated flow managers list
     * @return Processed data
     * @throws Exception
     */

    @POST
    public Map<StepId, ResourceProxy> apply(Process[] flow, @QueryParam("logic") String managerList) throws Exception {

        //Retrieve alternative managers name list
        String[] managersName = managerList!=null ? managerList.split(",") : new String[0];
        for (int i=0; i<managersName.length; i++)
            managersName[i] = managersName[i].trim();

        //Apply flow
        Map<StepId, Resource<DSDDataset,Object[]>> results = flowManager.process(flow,managersName);

        //Build response
        Map<StepId, ResourceProxy> response = new HashMap<>();
        if (results!=null)
            for (Map.Entry<StepId, Resource<DSDDataset,Object[]>> result : results.entrySet()) {
                Collection<Object[]> data = result.getValue().getData();
                org.fao.fenix.commons.msd.dto.templates.standard.combined.dataset.MetadataDSD metadataProxy = ResponseBeanFactory.getInstance(org.fao.fenix.commons.msd.dto.templates.standard.combined.dataset.MetadataDSD.class, result.getValue().getMetadata());
                response.put(result.getKey(), new ResourceProxy( metadataProxy, data, null, null, (long) data.size(), parameters.getLimit()));
            }

        return response;
    }

}
