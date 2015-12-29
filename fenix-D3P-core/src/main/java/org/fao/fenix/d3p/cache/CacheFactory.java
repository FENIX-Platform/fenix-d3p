package org.fao.fenix.d3p.cache;

import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.type.RepresentationType;
import org.fao.fenix.d3s.cache.manager.CacheResourceType;
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
        return cacheManagerFactory.getInstance(getCacheType(metadata), managerName!=null ? managerName : "dataset", storageName!=null ? storageName : "h2");
    }

    private CacheResourceType getCacheType(MeIdentification metadata) {
        MeContent content = metadata!=null ? metadata.getMeContent() : null;
        RepresentationType type = content!=null ? content.getResourceRepresentationType() : null;
        if (type!=null)
            switch (type) {
                case dataset: return CacheResourceType.dataset;
                case codelist: return CacheResourceType.codelist;
                case document: return CacheResourceType.document;
                default: return CacheResourceType.binary;
            }
        return CacheResourceType.dataset;
    }
}
