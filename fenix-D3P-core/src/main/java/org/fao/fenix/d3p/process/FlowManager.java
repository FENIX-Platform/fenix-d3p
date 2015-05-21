package org.fao.fenix.d3p.process;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3s.cache.D3SCache;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.*;

public class FlowManager {
    private @Inject StepFactory stepFactory;
    private @Inject ProcessFactory factory;
    private @Inject CacheFactory cacheFactory;
    private @Inject UIDUtils uidUtils;


    public Resource<org.fao.fenix.commons.msd.dto.full.DSDDataset,Object[]> process(MeIdentification<DSDDataset> metadata, org.fao.fenix.commons.process.dto.Process... flow) throws Exception {
        //Retrieve cache manager
        CacheManager<DSDDataset,Object[]> cacheManager = cacheFactory.getDatasetCacheManager(D3SCache.fixed);
        DatasetStorage cacheStorage = cacheManager!=null ? (DatasetStorage)cacheManager.getStorage() : null;
        Connection connection = cacheStorage!=null ? cacheStorage.getConnection() : null;
        if (connection==null)
            throw new UnsupportedOperationException("No cache available");

        //Retrieve source information
        String tableName = metadata!=null ? getId(metadata.getUid(), metadata.getVersion()) : null;
        DSDDataset dsd = metadata!=null ? metadata.getDsd() : null;
        if (tableName==null || dsd==null)
            return null;


        Stack<StatefulProcess> disposableProcesses = new Stack<>();
        Map<String, Step> steps = new HashMap<>();

        //Create source step
        Language[] languages = DatabaseStandards.getLanguageInfo();
        Step result = stepFactory.getInstance(StepType.table);
        result.setData(tableName);
        result.setRid(tableName);
        result.setDsd(languages!=null ? dsd.extend(languages) : dsd);
        steps.put(result.getRid(), result);

        try {

            //Run flow
            for (org.fao.fenix.commons.process.dto.Process processInfo : flow) {
                Process process = factory.getInstance(processInfo.getName());
                process.init(cacheStorage);
                if (process instanceof StatefulProcess)
                    disposableProcesses.push((StatefulProcess)process);

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

    private String getId(String uid, String version) {
        if (uid!=null)
            return version!=null ? uid+'|'+version : uid;
        else
            return null;
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
