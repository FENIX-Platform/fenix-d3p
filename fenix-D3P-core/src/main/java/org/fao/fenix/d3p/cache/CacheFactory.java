package org.fao.fenix.d3p.cache;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.d3s.cache.D3SCache;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.manager.CacheManagerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class CacheFactory {
    @Inject private CacheManagerFactory cacheManagerFactory;

    public CacheManager<DSDDataset, Object[]> getDatasetCacheManager(D3SCache cache) throws Exception {
        return cacheManagerFactory.getInstance(cache.name());
    }
}
