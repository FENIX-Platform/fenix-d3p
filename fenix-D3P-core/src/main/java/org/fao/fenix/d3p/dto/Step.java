package org.fao.fenix.d3p.dto;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeContent;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.msd.dto.type.RepresentationType;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

public abstract class Step<T> {
    private T data;
    private DSDDataset dsd;
    private String rid;


    public abstract StepType getType();
    public abstract Collection<Object[]> getData(Connection connection) throws Exception;

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public DSDDataset getDsd() {
        return dsd;
    }

    public void setDsd(DSDDataset dsd) {
        this.dsd = dsd;
    }

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }


    //Utils
    public Resource<DSDDataset,Object[]> getResource(Connection connection) throws Exception {
        return new Resource<>(getMetadata(), getData(connection));
    }
    public MeIdentification<DSDDataset> getMetadata () {
        MeContent content = new MeContent();
        content.setResourceRepresentationType(RepresentationType.dataset);

        MeIdentification<DSDDataset> metadata = new MeIdentification<>();
        metadata.setUid(getRid());
        metadata.setMeContent(content);
        metadata.setDsd(getDsd());

        return metadata;
    }

}
