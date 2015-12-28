package org.fao.fenix.d3p.cache;

import org.fao.fenix.commons.msd.dto.full.DSD;
import org.fao.fenix.commons.msd.dto.full.DSDCache;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.d3s.cache.D3SCache;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.manager.CacheManagerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class CacheFactory {
    @Inject private CacheManagerFactory cacheManagerFactory;

    public CacheManager<DSDDataset, Object[]> getDatasetCacheManager(MeIdentification metadata) throws Exception {
        DSD dsd = metadata!=null ? metadata.getDsd() : null;
        DSDCache cacheInfo = dsd!=null ? dsd.getCache() : null;
        String managerName = cacheInfo!=null ? cacheInfo.getManager() : null;
        String storageName = cacheInfo!=null ? cacheInfo.getStorage() : null;
        return cacheManagerFactory.getInstance(managerName!=null ? managerName : "dataset", storageName!=null ? storageName : "h2");
    }
}
