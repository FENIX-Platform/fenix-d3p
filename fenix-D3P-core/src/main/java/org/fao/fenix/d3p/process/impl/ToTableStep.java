package org.fao.fenix.d3p.process.impl;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.Iterator;

@ProcessName("asTable")
public class ToTableStep extends org.fao.fenix.d3p.process.CachedProcess {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;

    private String tableName;

    @Override
    public Step process(Connection connection, Object params, Step... sourceStep) throws Exception {
        //Reset bean level properties
        tableName = null;
        //Restireve source informations
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType sourceType = source!=null ? source.getType() : null;
        DSDDataset dsd = source!=null ? source.getDsd() : null;

        if (sourceType!=null && dsd!=null) {
            //Return source if it is already a table step
            if (sourceType==StepType.table)
                return source;
            //Retrieve new TMP table metadata
            DatasetStorage cacheStorage = getCacheStorage();
            tableName = getRandomTmpTableName();
            Table table = new Table(tableName, dsd);
            //Insert data into tmp table
            if (sourceType==StepType.iterator) {
                Iterator<Object[]> rawData = ((IteratorStep) source).getData();
                if (rawData != null) {
                    cacheStorage.create(table, null);
                    cacheStorage.store(table, databaseUtils.getDataIterator(rawData), 0, true);
                }
            } else if (sourceType==StepType.query) {
                String rawData = ((QueryStep)source).getData();
                if (rawData!=null && !rawData.trim().equals("")) {
                    cacheStorage.create(table, null);
                    connection.createStatement().executeUpdate("insert into "+getCacheStorage().getTableName(tableName)+' '+rawData);
                }
            }
            //Generate & return resulting step object
            TableStep step = (TableStep)stepFactory.getInstance(StepType.table);
            step.setRid(source.getRid() + "_cached");
            step.setData(tableName);
            step.setDsd(dsd);
            return step;
        } else
            throw new Exception("Source step to cache is unavailable or incomplete");
    }

    @Override
    public void dispose(Connection connection) throws Exception {
        getCacheStorage().delete(tableName);
    }
}
