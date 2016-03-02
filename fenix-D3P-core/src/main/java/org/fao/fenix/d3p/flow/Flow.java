package org.fao.fenix.d3p.flow;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;

public abstract class Flow {

    public abstract Resource<DSDDataset,Object[]> process(MeIdentification<DSDDataset> metadata, org.fao.fenix.commons.process.dto.Process... flow) throws Exception;

    //Utils
    public static String getId(String uid, String version) {
        if (uid!=null)
            return version!=null ? uid+'|'+version : uid;
        else
            return null;
    }


}
