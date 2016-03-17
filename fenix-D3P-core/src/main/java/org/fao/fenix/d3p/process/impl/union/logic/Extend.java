package org.fao.fenix.d3p.process.impl.union.logic;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.UnionJoin;
import org.fao.fenix.d3p.process.type.UnionLogicName;

import javax.enterprise.context.ApplicationScoped;
import java.util.*;

@UnionLogicName("extend")
@ApplicationScoped
public class Extend extends Base {

    @Override
    public QueryStep[] getUnionQuerySteps(Collection<Collection<Object[]>> sourcesByStorage, boolean labels) throws Exception {
        //Create destination table structure and conversion matrix
        ArrayList<DSDColumn> destinationColumns = new ArrayList<>();
        Collection<Collection<Integer[]>> transposeMatrixGroups = new LinkedList<>();

        Map<String, Integer> subjectsIndex = new HashMap<>();
        Map<String, Integer> idsIndex = new HashMap<>();
        for (Collection<Object[]> sources : sourcesByStorage) {
            Collection<Integer[]> transposeMatrixGroup = new LinkedList<>();
            for (Object[] sourceInfo : sources)
                transposeMatrixGroup.add(getTransposeMatrix(destinationColumns, (Step)sourceInfo[0], (UnionJoin) sourceInfo[1], subjectsIndex, idsIndex));
            transposeMatrixGroups.add(transposeMatrixGroup);
        }

        //Create resulting query steps
        return createQuerySteps(sourcesByStorage, destinationColumns, transposeMatrixGroups);
    }


    private Integer[] getTransposeMatrix(ArrayList<DSDColumn> destinationColumns, Step source, UnionJoin joinType, Map<String, Integer> subjectsIndex, Map<String, Integer> idsIndex) {
        Collection<Integer> matrix = new LinkedList<>();
        List<String> joinColumns = joinType.getColumns()!=null && joinType.getColumns().length>0 ? Arrays.asList(joinType.getColumns()) : null;

        for (DSDColumn column : source.getDsd().getColumns()) {
            Integer index = null;
            if (joinColumns==null || joinColumns.contains(column.getId())) {
                column.setKey(false);

                //Find match index
                index = getMatchIndex(column, source, matrix, destinationColumns.size(), joinType.getUsing(), subjectsIndex, idsIndex);
                //add as a new column or check and extend existing column domain
                if (index==null)
                    index = destinationColumns.size();
                if (index==destinationColumns.size()) {
                    destinationColumns.add(column);
                    idsIndex.put(column.getId(), index);
                    if (column.getSubject()!=null)
                        subjectsIndex.put(column.getSubject(), index);
                } else
                    extendDomain(source, column, destinationColumns.get(index));
            }
            //update transpose data
            matrix.add(index);
        }

        //Check for index duplication
        checkIndexDuplication(matrix, source, destinationColumns.size());

        //return matrix
        return matrix.toArray(new Integer[matrix.size()]);
    }

}
