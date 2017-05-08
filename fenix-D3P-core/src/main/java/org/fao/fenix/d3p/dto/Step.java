package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeContent;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.msd.dto.type.RepresentationType;
import org.fao.fenix.commons.process.dto.StepId;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public abstract class Step<T> {
    private T data;
    private DSDDataset dsd;
    private StepId rid;
    private DatasetStorage storage;
    private boolean oneToMany = false;


    public abstract StepType getType();
    public abstract Iterator<Object[]> getData(Connection connection) throws Exception;

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public DSDDataset getDsd() {
        //Prevent dsd overwrite in one to many relationship
        return oneToMany ? dsd.clone() : dsd;
    }
    public DSDDataset getCurrentDsd() {
        return dsd;
    }

    public void setDsd(DSDDataset dsd) {
        this.dsd = dsd;
    }

    public StepId getRid() {
        return rid;
    }

    public void setRid(StepId rid) {
        this.rid = rid;
    }

    public DatasetStorage getStorage() {
        return storage;
    }

    public void setStorage(DatasetStorage storage) {
        this.storage = storage;
    }

    public boolean isOneToMany() {
        return oneToMany;
    }

    public void setOneToMany(boolean oneToMany) {
        this.oneToMany = oneToMany;
    }

    //Utils
    public Resource<DSDDataset,Object[]> getResource() throws Exception {
        Connection connection = storage.getConnection();
        final Iterator<Object[]> rawData = getData(connection);
        return new Resource<>(getMetadata(), rawData != null ? new Collection<Object[]>() {
            @Override
            public int size() {
                return Integer.MAX_VALUE;
            }

            @Override
            public boolean isEmpty() {
                return !rawData.hasNext();
            }

            @Override
            public boolean contains(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<Object[]> iterator() {
                return rawData;
            }

            @Override
            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean add(Object[] objects) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean addAll(Collection<? extends Object[]> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }
        } : new LinkedList<Object[]>());
    }

    public MeIdentification<DSDDataset> getMetadata () {
        MeContent content = new MeContent();
        content.setResourceRepresentationType(RepresentationType.dataset);

        MeIdentification<DSDDataset> metadata = new MeIdentification<>();
        metadata.setUid(rid.getUid());
        metadata.setVersion(rid.getVersion());
        metadata.setMeContent(content);
        metadata.setDsd(getCurrentDsd());

        return metadata;
    }

}
