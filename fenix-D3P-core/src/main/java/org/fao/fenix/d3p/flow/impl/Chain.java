package org.fao.fenix.d3p.flow.impl;


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
import org.fao.fenix.d3p.flow.Flow;
import org.fao.fenix.d3p.process.Process;
import org.fao.fenix.d3p.process.ProcessFactory;
import org.fao.fenix.d3p.process.DisposableProcess;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class Chain extends Flow {
    private @Inject StepFactory stepFactory;
    private @Inject ProcessFactory factory;
    private @Inject CacheFactory cacheFactory;
    private @Inject UIDUtils uidUtils;


    private boolean isSingleChain(org.fao.fenix.commons.process.dto.Process[] flow) {
        for (int i=1; i<flow.length; i++)
            if (flow[i].getSid().length>1 || !flow[i].getSid()[0].equals(flow[i-1].getRid()))
                return false;
        return true;
    }


    @Override
    public Resource<DSDDataset,Object[]> process(Map<StepId,TableStep> sourceSteps, Map<String, Process> processes, org.fao.fenix.commons.process.dto.Process[] flow) throws Exception {
        //Verify applicability
        if (isSingleChain(flow))
            throw new UnsupportedOperationException();


        //Retrieve cache manager
        CacheManager<DSDDataset,Object[]> cacheManager = cacheFactory.getDatasetCacheManager(metadata);
        DatasetStorage cacheStorage = cacheManager!=null ? (DatasetStorage)cacheManager.getStorage() : null;

        //Retrieve source information
        String tableName = metadata!=null ? getId(metadata.getUid(), metadata.getVersion()) : null;
        DSDDataset dsd = metadata!=null ? metadata.getDsd() : null;
        if (tableName==null || dsd==null)
            return null;


        Stack<DisposableProcess> disposableProcesses = new Stack<>();
        Map<String, Step> steps = new HashMap<>();

        //Create source step
        Language[] languages = DatabaseStandards.getLanguageInfo();
        Step result = stepFactory.getInstance(StepType.table);
        result.setData(cacheStorage.getTableName(tableName));
        result.setRid(tableName);
        result.setDsd(languages!=null ? dsd.extend(false, languages) : dsd);
        steps.put(result.getRid(), result);

        Connection connection = cacheStorage!=null ? cacheStorage.getConnection() : null;
        if (connection==null)
            throw new UnsupportedOperationException("No cache available");

        try {

            //Run flow
            for (org.fao.fenix.commons.process.dto.Process processInfo : flow) {
                Process process = factory.getInstance(processInfo.getName());
                process.init(cacheStorage);
                if (process instanceof DisposableProcess)
                    disposableProcesses.push((DisposableProcess)process);

                result = process.process(connection, processInfo.getParameters(), getSources(processInfo, result, steps));
                setRid(result, processInfo);

                steps.put(result.getRid(), result);
            }

            //Generate and return in-memory resource from the last step
            return result.getResource(connection);
        } finally {
            //Close data retrieve connection to unlock tables
            connection.close();
            //Dispose cached steps
            connection = cacheStorage.getConnection();
            for (int size = disposableProcesses.size(); size>0; size--)
                try {
                    disposableProcesses.pop().dispose(connection);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            connection.close();
        }
    }


    //Utils
    private void setRid(Step step, org.fao.fenix.commons.process.dto.Process processInfo) {
        if (step!=null && step.getRid()==null) {
            String rid = processInfo!=null ? processInfo.getRid() : null;
            step.setRid(rid==null ? uidUtils.getId() : null);
        }
    }


    private Step[] getSources (org.fao.fenix.commons.process.dto.Process processInfo, Step lastStep, Map<String, Step> steps) throws Exception {
        String[] sourcesId = processInfo!=null ? processInfo.getSid() : null;
        if (sourcesId!=null && sourcesId.length>0) {
            Step[] sources = new Step[sourcesId.length];
            for (int i=0; i<sourcesId.length; i++)
                if ((sources[i] = steps.get(sourcesId[i])) == null)
                    throw new Exception("Source unavailable for current process step.");
            return sources;
        }
        return lastStep!=null ? new Step[]{lastStep} : null;
    }

}
