package org.fao.fenix.d3p.process.impl.join.logic;

import com.sun.corba.se.spi.logging.CORBALogDomains;
import org.fao.fenix.commons.msd.dto.full.DSD;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.templates.identification.DSDDataset;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.JoinParameter;
import org.fao.fenix.d3p.process.dto.JoinParams;
import org.fao.fenix.d3p.process.dto.JoinTypes;
import org.fao.fenix.d3p.process.impl.join.JoinLogic;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Link;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ManualJoin implements JoinLogic {

    private JoinParams params;

    public ManualJoin(JoinParams params) {
        this.params = params;
    }


    @Override
    public Step[] process(Step... sourceStep) {

        // check that parameters follows the sid

        validate(sourceStep);

        StringBuilder query;




        return new Step[0];
    }

    private void validate(Step... steps) {
        if (this.params.getJoins().size() == steps.length) {
            Iterator<Collection<JoinParameter>> it =  this.params.getJoins().iterator();
            int sizeFirst = it.next().size();
            while (it.hasNext()) {
                if (sizeFirst != it.next().size()) {
                    throw new BadRequestException("wrong configuration for join parameters: the number should be the same for each resource");
                }
            }
        } else {
            throw new BadRequestException("wrong configuration for join parameters: the number should be equal to the number of resources");
        }
    }

    private List<DSDColumn> createDSDColumns (Step... sourceStep) {


        // create key Columns
        List<DSDColumn> resultColumns = new ArrayList<DSDColumn>();
        fillKeyColumns(resultColumns,sourceStep);

        

    }

    private void fillKeyColumns (List<DSDColumn> columns, Step... sources) {

        JoinParameter[][] parameters = (JoinParameter[][]) this.params.getJoins().toArray();
        for(int i=0, keyColumns = parameters[0].length; i<keyColumns; i++) {
            // search id for each dataset
            for(int j=0, sidLength = parameters.length; j<sidLength; j++){
                if(parameters[j][i].getType() == JoinTypes.id) {
                    DSDColumn column = find(parameters[j][i].getValue().toString(), sources[j].getDsd());
                    if(column!=null)
                        columns.add(column);
                }
            }
        }

        if (columns.size() <=0)
            throw new BadRequestException("wrong configuration for join parameters:id parameters does not exists");
    }


    private DSDColumn find(String columnID, org.fao.fenix.commons.msd.dto.full.DSDDataset dsdDataset) {
        for(DSDColumn column: dsdDataset.getColumns()){
            if(column.getId().equals(columnID))
                return column;
        }
        return null;
    }





}
