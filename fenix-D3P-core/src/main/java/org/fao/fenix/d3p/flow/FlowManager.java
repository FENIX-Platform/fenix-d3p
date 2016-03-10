package org.fao.fenix.d3p.flow;


import org.apache.log4j.Logger;
import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.process.dto.StepId;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.dto.TableStep;
import org.fao.fenix.d3p.process.Process;
import org.fao.fenix.d3p.process.ProcessFactory;
import org.fao.fenix.d3p.process.DisposableProcess;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.storage.Storage;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;
import org.fao.fenix.d3s.msd.services.spi.Resources;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.util.*;

@ApplicationScoped
public class FlowManager {
    private static final Logger LOGGER = Logger.getLogger(FlowManager.class);

    private @Inject Instance<Flow> flowManagersInstance;

    private @Inject StepFactory stepFactory;
    private @Inject ProcessFactory factory;
    private @Inject CacheFactory cacheFactory;
    private @Inject UIDUtils uidUtils;

    private @Inject Resources resourcesService;


    //Throws UnsupportedOperationException when all compatible flow managers cannot apply flow process
    public Map<StepId, Resource<DSDDataset,Object[]>> process(org.fao.fenix.commons.process.dto.Process[] flow, String ... names) throws Exception {
        //Flow operations
        normalize(flow);
        validate(flow);
        //Fetch operations
        Map<StepId,TableStep> sourceSteps = new HashMap<>();
        Set<StepId> resultRidList = new HashSet<>();
        load_SourceSteps_ResultRidList(flow,sourceSteps,resultRidList);

        Process[] processes = new Process[flow.length];
        List<DisposableProcess> disposableProcesses = new LinkedList<>();
        loadProcessesInstance(flow,processes,disposableProcesses);

        //Apply flow
        for (Flow flowManager : getAvailableManagers(names))
            try {
                return flowManager.process(sourceSteps, resultRidList, processes, flow);
            } catch (UnsupportedOperationException ex) {
                //Nothing to do here to try with the next flow manager
            } finally {
                //Close disposable processes (LIFO)
                Collections.reverse(disposableProcesses);
                boolean error = false;
                for (DisposableProcess process : disposableProcesses)
                    try {
                        process.dispose();
                    } catch (Exception ex) {
                        error = true;
                        LOGGER.error("Error disposing process "+process.getClass().getName(),ex);
                    }
                if (error)
                    throw new Exception("Error disposing one or more flow process");
            }
        //Throw error when no manager can apply required flow
        throw new UnsupportedOperationException("None of the available/required flow managers can execute the work flow");
    }


    //Normalization
    private void normalize(org.fao.fenix.commons.process.dto.Process[] flow) throws Exception {
        //Assign unavailable rid
        for (int i=0; i<flow.length; i++)
            if (flow[i].getRid()==null)
                flow[i].setRid(new StepId("D3P_R_"+i,uidUtils.newId()));
        //Assign unavailable sid
        for (int i=1; i<flow.length; i++)
            if (flow[i].getSid()==null || flow[i].getSid().length==0)
                flow[i].setSid(new StepId[]{flow[i-1].getRid()});
        //Assign processes index (useful as a unique ID)
        for (int i=1; i<flow.length; i++)
            flow[i].index = i;
    }

    //Validation
    private void validate(org.fao.fenix.commons.process.dto.Process[] flow) throws Exception {
        //Check first process SID availability
        if (flow[0].getSid()==null || flow[0].getSid().length==0)
            throw new BadRequestException("First process must declare a source.");
        //Check SID uniqueness at process level
        for (int i=1; i<flow.length; i++)
            if (flow[i].getSid().length != new HashSet<>(Arrays.asList(flow[i].getSid())).size())
                throw new BadRequestException("Process SID must be unique for the process itself. The duplication is into the process at index "+i);
        //Check RID uniqueness at flow level
        Set<StepId> ridSet = new HashSet<>();
        for (org.fao.fenix.commons.process.dto.Process processInfo : flow)
            if (!ridSet.add(processInfo.getRid()))
                throw new BadRequestException("Processes RID must be unique into the flow. The duplication is: "+processInfo.getRid());
        //Check for cycle presence starting from processes using one or more source dataset
        Map<StepId,Collection<org.fao.fenix.commons.process.dto.Process>> processesInfoBySid = new HashMap<>();
        for (org.fao.fenix.commons.process.dto.Process processInfo : flow)
            for (StepId sid : processInfo.getSid()) {
                if (processesInfoBySid.containsKey(sid))
                    processesInfoBySid.put(sid, new LinkedList<org.fao.fenix.commons.process.dto.Process>());
                processesInfoBySid.get(sid).add(processInfo);
            }

        Set<StepId> sourceSidSet = new HashSet<>(processesInfoBySid.keySet());
        sourceSidSet.removeAll(ridSet);

        Stack<org.fao.fenix.commons.process.dto.Process> chain = new Stack<>();
        for (StepId sourceSid : sourceSidSet)
            for (org.fao.fenix.commons.process.dto.Process processInfo : processesInfoBySid.get(sourceSid)) {
                chain.push(processInfo);
                org.fao.fenix.commons.process.dto.Process cycleProcess = getCycleProcess(processesInfoBySid,chain);
                if (cycleProcess!=null)
                    throw new BadRequestException("Processes RID must be unique into the flow. The duplication is on the process "+cycleProcess+" starting from process "+processInfo);
            }

    }
    private org.fao.fenix.commons.process.dto.Process getCycleProcess(
            Map<StepId,Collection<org.fao.fenix.commons.process.dto.Process>> processesInfoBySid,
            Stack<org.fao.fenix.commons.process.dto.Process> chain) throws Exception {

        org.fao.fenix.commons.process.dto.Process processInfo = chain.peek();
        org.fao.fenix.commons.process.dto.Process cycleProcess = null;

        if (chain.contains(processInfo))
            cycleProcess = processInfo;
        else {
            Collection<org.fao.fenix.commons.process.dto.Process> nextProcesses = processesInfoBySid.get(processInfo.getRid());
            if (nextProcesses!=null)
                for (org.fao.fenix.commons.process.dto.Process nextProcess : nextProcesses) {
                    chain.push(nextProcess);
                    if ((cycleProcess = getCycleProcess(processesInfoBySid, chain))!=null)
                        break;
                }
        }

        chain.pop();
        return cycleProcess;
    }

    //Fetch utils
    private void load_SourceSteps_ResultRidList(org.fao.fenix.commons.process.dto.Process[] flow, Map<StepId,TableStep> sourceStepMap, Set<StepId> resultRidList) throws Exception {
        //Retrieve sources linked to datasets not included into the flow
        Set<StepId> sources = new HashSet<>();
        Set<StepId> results = new HashSet<>();
        for (org.fao.fenix.commons.process.dto.Process processInfo : flow) {
            sources.addAll(Arrays.asList(processInfo.getSid()));
            results.add(processInfo.getRid());
        }
        //Retrieve results rid
        resultRidList.addAll(results);
        resultRidList.removeAll(sources);
        for (org.fao.fenix.commons.process.dto.Process processInfo : flow)
            if (processInfo.isResult())
                resultRidList.add(processInfo.getRid());
        //Retrieve dataset source steps
        sources.removeAll(results);
        for (StepId sourceId : sources)
            sourceStepMap.put(sourceId,createSourceStep(sourceId));
    }
    private TableStep createSourceStep (StepId stepId) throws Exception {
        //Retrieve source metadata
        MeIdentification<DSDDataset> metadata = resourcesService.loadMetadata(stepId.getUid(),stepId.getVersion());
        if (metadata==null)
            throw new NotFoundException("Source dataset not found: uid = "+stepId.getUid()+" - version = "+stepId.getVersion());
        //Fetch source data
        resourcesService.fetch(metadata);
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
        DSDDataset dsd = metadata.getDsd();
        if (languages!=null)
            dsd.extend(languages);
        step.setDsd(dsd);
        step.setStorage((DatasetStorage) cacheStorage);
        //Return source step
        return step;
    }
    private void loadProcessesInstance (org.fao.fenix.commons.process.dto.Process[] flow, Process[] processes, Collection<DisposableProcess> disposableProcesses) throws Exception {
        for (int i=0; i<flow.length; i++) {
            processes[i] = factory.getInstance(flow[i].getName());
            if (processes[i] instanceof DisposableProcess)
                disposableProcesses.add((DisposableProcess) processes[i]);
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
            Map<Integer, Flow> flowManagersByPriority = new TreeMap<>();
            for (Flow flowManager : availableFlowManagers) {
                FlowProperties flowProperties = flowManager.getClass().getAnnotation(FlowProperties.class);
                if (flowProperties != null && flowProperties.global())
                    flowManagersByPriority.put(flowProperties.priority(), flowManager);
            }

            flowManagers.addAll(flowManagersByPriority.values());
        }

        return flowManagers;
    }


}
