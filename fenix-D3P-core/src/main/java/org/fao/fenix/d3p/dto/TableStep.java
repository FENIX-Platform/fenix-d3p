package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.templates.identification.DSDCodelist;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3s.cache.D3SCache;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;

public class TableStep extends Step<String> {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject CacheFactory cacheFactory;


    @Override
    public StepType getType() {
        return StepType.table;
    }

    @Override
    public Iterator<Object[]> getData(Connection connection) throws Exception {
        ResultSet rawData = connection.createStatement().executeQuery(getQuery());
        return new DataIterator(rawData, null, null, null);
    }

    //Utils
    public QueryStep toQueryStep() throws Exception {
        QueryStep step = new QueryStep();
        step.setRid(getRid());
        step.setDsd(getDsd());
        step.setData(getQuery());
        return step;
    }

    public String getQuery() throws Exception {
        CacheManager<DSDDataset,Object[]> cacheManager = cacheFactory.getDatasetCacheManager(D3SCache.fixed);
        DatasetStorage cacheStorage = cacheManager!=null ? (DatasetStorage)cacheManager.getStorage() : null;
        if (cacheStorage==null)
            throw new UnsupportedOperationException("No cache available");

        StringBuilder query = new StringBuilder();
        for (DSDColumn column : getDsd().getColumns())
            query.append(',').append(column.getId());
        return "select "+query.substring(1)+" from "+cacheStorage.getTableName(getData());
    }

}
