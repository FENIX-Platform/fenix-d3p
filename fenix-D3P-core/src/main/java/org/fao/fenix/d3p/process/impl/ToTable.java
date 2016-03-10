package org.fao.fenix.d3p.process.impl;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.DisposableProcess;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.dto.dataset.TableScope;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.Iterator;

@ProcessName("asTable")
public class ToTable extends DisposableProcess {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;

    private String tableName;
    private DatasetStorage cacheStorage;

    @Override
    public Step process(Object params, Step... sourceStep) throws Exception {
        //Restireve source informations
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType sourceType = source!=null ? source.getType() : null;
        if (sourceType==null)
            throw new UnsupportedOperationException("toTable process can be applied only on one input");
        //Prepare metadata
        tableName=getRandomTmpTableName();
        DSDDataset dsd = source.getDsd();
        Table table = new Table(tableName, dsd);
        //Create destination temporary table
        cacheStorage = source.getStorage();
        cacheStorage.create(table, null, TableScope.temporary);
        //Generate resulting step object
        TableStep step = (TableStep)stepFactory.getInstance(StepType.table);
        step.setData(cacheStorage.getTableName(tableName));
        step.setDsd(dsd);
        //Fill destination table with source data
        Connection connection = cacheStorage.getConnection();
        try {
            switch (sourceType) {
                case table:
                    connection.createStatement().executeUpdate("INSERT INTO " + step.getData() + " SELECT * FROM " + source.getData());
                    break;
                case query:
                    databaseUtils.fillStatement(connection.prepareStatement("INSERT INTO " + step.getData() + ' ' + source.getData()), ((QueryStep)source).getTypes(), ((QueryStep)source).getParams()).executeUpdate();
                    break;
                case iterator:
                    cacheStorage.store(table, ((IteratorStep) source).getData(), 0, true, new Date());
                    break;
            }
        } finally {
            connection.close();
        }
        //Fill step
        return step;
    }

    @Override
    public void dispose() throws Exception {
        cacheStorage.delete(tableName);
    }
}
