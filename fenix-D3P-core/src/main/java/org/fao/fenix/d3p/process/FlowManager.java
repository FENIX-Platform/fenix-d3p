package org.fao.fenix.d3p.process;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.process.dto.*;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.TebleStep;
import org.fao.fenix.d3s.cache.D3SCache;
import org.fao.fenix.d3s.cache.dto.StoreStatus;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.manager.CacheManagerFactory;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class FlowManager {
    @Inject ProcessFactory factory;
    @Inject CacheFactory cacheFactory;

    public Resource<org.fao.fenix.commons.msd.dto.full.DSDDataset,Object[]> process(MeIdentification<DSDDataset> metadata, org.fao.fenix.commons.process.dto.Process... flow) throws Exception {
        if (metadata!=null) {
            //Retrieve cache manager
            CacheManager<DSDDataset,Object[]> cacheManager = cacheFactory.getDatasetCacheManager(D3SCache.fixed);
            if (cacheManager==null)
                throw new UnsupportedOperationException("No cache available");
            DatasetStorage cacheStorage = (DatasetStorage)cacheManager.getStorage();
            Connection connection = cacheStorage.getConnection();


            //Retrieve source information
            DSDDataset dsd = metadata.getDsd();
            StoreStatus tableStatus = cacheManager.status(metadata);

            //Run flow
            String tableName = getId(metadata.getUid(), metadata.getVersion());
            Step result = new TebleStep();
            result.setData(tableName);
            result.setRid(tableName);
            result.setDsd(metadata.getDsd());
            Map<String, Step> steps = new HashMap<>();
            steps.put(result.getRid(),result);

            for (org.fao.fenix.commons.process.dto.Process processInfo : flow) {
                Process process = factory.getInstance(processInfo.getName());
                result = process.process(processInfo.getParameters(), getSources(processInfo, result, steps));

                setRid(result, processInfo);
                steps.put(result.getRid(), result);
            }

            //TODO retrieve resource from a Step
        }
        return null;
    }



    //Utils
    private String getId(String uid, String version) {
        if (uid!=null)
            return version!=null ? uid+'|'+version : uid;
        else
            return null;
    }

    private void setRid(Step step, org.fao.fenix.commons.process.dto.Process processInfo) {
        String rid = processInfo!=null ? processInfo.getRid() : null;
        rid = rid==null && step!=null ? step.getRid() : null;
        rid = rid==null && processInfo!=null ? processInfo.getName() : null;

        step.setRid(rid);
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
