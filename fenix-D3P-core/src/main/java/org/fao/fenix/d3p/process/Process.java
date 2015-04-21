package org.fao.fenix.d3p.process;



import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.inject.Inject;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;

public abstract class Process <T> {
    @Inject UIDUtils uidUtils;

    private DatasetStorage cacheStorage;
    public final void init (DatasetStorage cacheStorage) {
        this.cacheStorage = cacheStorage;
    }

    /**
     * Get the expected external parameters Java type. This method is used to parse JSON payload.
     * @return Parameters Java type
     */
    public Type getParametersType() {
        return ((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * Execute the process. It's a synchronous activity.
     * @param sourceStep previous step
     * @param params Current process external parameters.
     * @return
     */
    public abstract Step process(Connection connection, T params, Step ... sourceStep);


    //UTILS
    protected String getRandomTmpTableName() {
        return cacheStorage.getTemporaryTableName(uidUtils.getId());
    }

}
