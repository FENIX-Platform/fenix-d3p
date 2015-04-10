package org.fao.fenix.d3p.dao;

import org.fao.fenix.d3s.cache.D3SCache;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.manager.CacheManagerFactory;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;

public class StorageFactory {
    private @Inject CacheManagerFactory cacheFactory;


    public DatasetStorage getDatasetStorage() throws Exception {
        CacheManager cacheManager = cacheFactory.getInstance(D3SCache.fixed.name());
        return (DatasetStorage)cacheManager.getStorage();
    }
    public CacheManager getCacheManager() throws Exception {
        return cacheFactory.getInstance(D3SCache.fixed.name());
    }
}
