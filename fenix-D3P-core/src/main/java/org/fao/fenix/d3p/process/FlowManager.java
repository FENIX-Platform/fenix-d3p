package org.fao.fenix.d3p.process;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.templates.export.combined.dataset.DSD;
import org.fao.fenix.commons.msd.dto.templates.export.dsd.DSDDataset;

public class FlowManager {

    public Resource<org.fao.fenix.commons.msd.dto.full.DSDDataset,Object[]> init(DSD metadata, Process ... processes) {
        if (metadata!=null) {
            String tableName = getId(metadata.getUid(), metadata.getVersion());
            DSDDataset dsd = metadata.getDsd();
        }
        return null;
    }



    //Utils
    private String getId(String uid, String version) {
        if (uid!=null)
            return version!=null ? uid+'|'+version : uid;
        else
            return null;
    }


}
