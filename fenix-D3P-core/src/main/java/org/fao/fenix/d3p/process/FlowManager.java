package org.fao.fenix.d3p.process;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.TebleStep;
import org.fao.fenix.d3s.cache.D3SCache;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.*;

public class FlowManager {
    private @Inject ProcessFactory factory;
    private @Inject CacheFactory cacheFactory;

    public Resource<org.fao.fenix.commons.msd.dto.full.DSDDataset,Object[]> process(MeIdentification<DSDDataset> metadata, org.fao.fenix.commons.process.dto.Process... flow) throws Exception {
        //Retrieve cache manager
        CacheManager<DSDDataset,Object[]> cacheManager = cacheFactory.getDatasetCacheManager(D3SCache.fixed);
        DatasetStorage cacheStorage = cacheManager!=null ? (DatasetStorage)cacheManager.getStorage() : null;
        Connection connection = cacheStorage!=null ? cacheStorage.getConnection() : null;
        if (connection==null)
            throw new UnsupportedOperationException("No cache available");

        //Retrieve source information
        String tableName = metadata!=null ? cacheStorage.getTableName(getId(metadata.getUid(), metadata.getVersion())) : null;
        DSDDataset dsd = metadata!=null ? metadata.getDsd() : null;
        if (tableName==null || dsd==null)
            return null;


        Stack<CachedProcess> disposableProcesses = new Stack<>();
        Map<String, Step> steps = new HashMap<>();

        //Create source step
        Step result = new TebleStep();
        result.setData(tableName);
        result.setRid(tableName);
        result.setDsd(dsd);
        steps.put(result.getRid(), result);

        try {

            //Run flow
            for (org.fao.fenix.commons.process.dto.Process processInfo : flow) {
                Process process = factory.getInstance(processInfo.getName());

                process.init(cacheStorage);
                result = process.process(connection, processInfo.getParameters(), getSources(processInfo, result, steps));

                if (process instanceof CachedProcess)
                    disposableProcesses.push((CachedProcess)process);
                steps.put(result.getRid(), result);
            }

            //Generate and return in-memory resource from the last step
            return result.getResource(connection);
        } finally {
            for (CachedProcess process = disposableProcesses.pop(); process!=null; process = disposableProcesses.pop())
                try {
                    process.dispose(connection);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            connection.close();
        }
    }


    //Utils
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
