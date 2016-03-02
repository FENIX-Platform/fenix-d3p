package org.fao.fenix.d3p.flow;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.process.dto.StepId;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.dto.TableStep;
import org.fao.fenix.d3p.process.Process;
import org.fao.fenix.d3p.process.ProcessFactory;
import org.fao.fenix.d3p.process.StatefulProcess;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.storage.Storage;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;
import org.fao.fenix.d3s.msd.services.spi.Resources;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import java.util.*;

@ApplicationScoped
public class FlowManager {
    @Inject Instance<Flow> flowManagersInstance;
    private @Inject StepFactory stepFactory;
    private @Inject ProcessFactory factory;
    private @Inject CacheFactory cacheFactory;
    private @Inject UIDUtils uidUtils;

    private @Inject Resources resourcesService;


//TODO modificare la firma richiedendo solo i processi
//TODO normalizzazione dei processi
//TODO validazione dei processi


    //Throws UnsupportedOperationException when all compatible flow managers cannot apply flow process
    public Resource<DSDDataset,Object[]> process(MeIdentification<DSDDataset> metadata, org.fao.fenix.commons.process.dto.Process[] flow, String ... names) throws Exception {
        //Flow operations
        normalize(flow);
        validate(flow);
        //Fetch operations
        Map<StepId,TableStep> sourceSteps = getSourceSteps(flow);
        Map<String, Process> processes = new HashMap<>();
        Stack<StatefulProcess> disposableProcesses = new Stack<>();
        loadProcessesInstance(flow,processes,disposableProcesses);

        //Apply flow
        for (Flow flowManager : getAvailableManagers(names))
            try {
                return flowManager.process(metadata, flow);
            } catch (UnsupportedOperationException ex) {
                //Nothing to do here to try with the next flow manager
            } finally {
                //Close disposable processes
                for (int size = disposableProcesses.size(); size>0; size--) {
                    boolean error = false;
                    try {
                        //TODO without connection (it can be taken from Process.storage property)
                        //disposableProcesses.pop().dispose(connection);
                    } catch (Exception ex) {
                        error = true;
                        ex.printStackTrace();
                    }
                }
            }
        //Throw error when no manager can apply required flow
        throw new UnsupportedOperationException("None of the available/required flow managers can execute the work flow");
    }


    //Flow metadata utils
    private void normalize(org.fao.fenix.commons.process.dto.Process[] flow) throws Exception {

    }
    private void validate(org.fao.fenix.commons.process.dto.Process[] flow) throws Exception {

    }

    private Map<StepId,TableStep> getSourceSteps(org.fao.fenix.commons.process.dto.Process[] flow) throws Exception {
        //Retrieve sources linked to datasets not included into the flow
        Set<StepId> sources = new HashSet<>();
        Set<StepId> results = new HashSet<>();
        for (org.fao.fenix.commons.process.dto.Process processInfo : flow) {
            sources.addAll(Arrays.asList(processInfo.getSid()));
            results.add(processInfo.getRid());
        }
        sources.removeAll(results);
        //Return dataset source steps
        Map<StepId,TableStep> sourceStepMap = new HashMap<>();
        for (StepId sourceId : sources)
            sourceStepMap.put(sourceId,createSourceStep(sourceId));
        return sourceStepMap;
    }
    private TableStep createSourceStep (StepId stepId) throws Exception {
        //Retrieve source metadata
        MeIdentification<DSDDataset> metadata = resourcesService.loadMetadata(stepId.getUid(),stepId.getVersion());
        if (metadata==null)
            throw new NotFoundException("Source dataset not found: uid = "+stepId.getUid()+" - version = "+stepId.getVersion());
        //Retrieve cache manager
        CacheManager cacheManager = cacheFactory.getDatasetCacheManager(metadata);
        Storage cacheStorage = cacheManager!=null ? cacheManager.getStorage() : null;
        if (cacheStorage==null || !(cacheStorage instanceof DatasetStorage))
            throw new UnsupportedOperationException("Cache storage unavailable for source dataset: uid = "+stepId.getUid()+" - version = "+stepId.getVersion());
        //Retrieve required languages
        Language[] languages = DatabaseStandards.getLanguageInfo();
        //Create source step
        TableStep step = (TableStep)stepFactory.getInstance(StepType.table);
        step.setData(((DatasetStorage)cacheStorage).getTableName(Flow.getId(stepId.getUid(),stepId.getVersion())));
        step.setRid(stepId);
        step.setDsd(languages!=null ? metadata.getDsd().extend(false, languages) : metadata.getDsd());
        step.setStorage((DatasetStorage) cacheStorage);
        //Return source step
        return step;
    }
    private void loadProcessesInstance (org.fao.fenix.commons.process.dto.Process[] flow, Map<String, Process> processes, Stack<StatefulProcess> disposableProcesses) throws Exception {
        for (org.fao.fenix.commons.process.dto.Process processInfo : flow) {
            Process process = factory.getInstance(processInfo.getName());
            if (process instanceof StatefulProcess)
                disposableProcesses.push((StatefulProcess) process);

            processes.put(processInfo.getName(), process);
        }
    }

    //Factory utils

    private Collection<Flow> getAvailableManagers(String ... names) {
        Collection<Flow> flowManagers = new LinkedList<>();
        Iterable<Flow> availableFlowManagers = new Iterable<Flow>() {
            @Override
            public Iterator<Flow> iterator() {
                return flowManagersInstance.select().iterator();
            }
        };

        if (names!=null && names.length>0) {
            Map<String,Flow> flowManagersMap = new HashMap<>();
            for (Flow flowManager : availableFlowManagers) {
                FlowProperties flowProperties = flowManager.getClass().getAnnotation(FlowProperties.class);
                String name = flowProperties!=null ? flowProperties.name() : null;
                if (name!=null)
                    flowManagersMap.put(name, flowManager);
            }

            for (String name : names)
                if (flowManagersMap.containsKey(name))
                    flowManagers.add(flowManagersMap.get(name));
        } else {
            List<Object[]> flowManagersPriority = new LinkedList<>();
            for (Flow flowManager : availableFlowManagers) {
                FlowProperties flowProperties = flowManager.getClass().getAnnotation(FlowProperties.class);
                if (flowProperties!=null && flowProperties.global())
                    flowManagersPriority.add(new Object[]{flowProperties.priority(), flowManager});
            }

            Collections.sort(flowManagersPriority, new Comparator<Object[]>() {
                @Override
                public int compare(Object[] o1, Object[] o2) {
                    return ((Integer)o1[0]).compareTo((Integer)o2[0]);
                }
            });

            for (Object[] flowManager : flowManagersPriority)
                flowManagers.add((Flow)flowManager[1]);
        }

        return flowManagers;
    }


}
