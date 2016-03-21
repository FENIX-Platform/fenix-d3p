package org.fao.fenix.d3p.process.impl.union.logic;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.UnionJoin;
import org.fao.fenix.d3p.process.type.UnionLogicName;

import javax.enterprise.context.ApplicationScoped;
import java.util.*;

@UnionLogicName("intersect")
@ApplicationScoped
public class Intersect extends Base {

    @Override
    public QueryStep[] getUnionQuerySteps(Collection<Collection<Object[]>> sourcesByStorage, boolean labels) throws Exception {
        //Create destination table structure and conversion matrix
        ArrayList<DSDColumn> destinationColumns = new ArrayList<>();
        Collection<Collection<Integer[]>> transposeMatrixGroups = new LinkedList<>();

        Map<String, Integer> subjectsIndex = new HashMap<>();
        Map<String, Integer> idsIndex = new HashMap<>();
        int[] counters = null;
        int size = 0;
        for (Collection<Object[]> sources : sourcesByStorage) {
            Collection<Integer[]> transposeMatrixGroup = new LinkedList<>();
            for (Object[] sourceInfo : sources) {
                //Add matrix
                Integer[] matrix = getTransposeMatrix(destinationColumns, (Step) sourceInfo[0], (UnionJoin) sourceInfo[1], subjectsIndex, idsIndex);
                transposeMatrixGroup.add(matrix);
                //Update counters
                if (counters==null) {
                    counters = new int[matrix.length];
                    for (Integer index : matrix)
                        counters[index]++;
                } else
                    for (Integer index : matrix)
                        if (index!=null)
                            counters[index]++;
                size++;
            }
            transposeMatrixGroups.add(transposeMatrixGroup);
        }

        //Clean destination structure and matrix
        Integer[] conversionMatrix = new Integer[counters.length];
        int removedCount=0;
        for (int i=0; i<counters.length; i++)
            if (counters[i]<size) {
                destinationColumns.remove(i-removedCount++);
                conversionMatrix[i] = null;
            } else
                conversionMatrix[i] = i-removedCount;

        for (Collection<Integer[]> transposeMatrixGroup : transposeMatrixGroups)
            for (Integer[] matrix : transposeMatrixGroup)
                for (int i=0; i<matrix.length; i++)
                    if (matrix[i]!=null)
                        matrix[i] = conversionMatrix[matrix[i]];

        //Create resulting query steps
        return createQuerySteps(sourcesByStorage, destinationColumns, transposeMatrixGroups);
    }

    private Integer[] getTransposeMatrix(ArrayList<DSDColumn> destinationColumns, Step source, UnionJoin joinType, Map<String, Integer> subjectsIndex, Map<String, Integer> idsIndex) {
        Collection<Integer> matrix = new LinkedList<>();
        List<String> joinColumns = joinType!=null && joinType.getColumns()!=null && joinType.getColumns().length>0 ? Arrays.asList(joinType.getColumns()) : null;

        if (destinationColumns.size()==0) {
            int i=0;
            for (DSDColumn column : source.getDsd().getColumns()) {
                if (joinColumns==null || joinColumns.contains(column.getId())) {
                    column.setKey(false);
                    destinationColumns.add(column);
                    idsIndex.put(column.getId(), i);
                    if (column.getSubject() != null)
                        subjectsIndex.put(column.getSubject(), i);
                    matrix.add(i++);
                }
            }
        } else {
            for (DSDColumn column : source.getDsd().getColumns()) {
                Integer index = null;
                if (joinColumns==null || joinColumns.contains(column.getId())) {
                    //Find match index
                    index = getMatchIndex(column, source, matrix, destinationColumns.size(), joinType!=null ? joinType.getUsing() : UnionJoin.DEFAULT_JOIN_TYPE, subjectsIndex, idsIndex);
                    //check and extend existing column domain
                    if (index!=null)
                        extendDomain(source, column, destinationColumns.get(index));
                }
                //update transpose data
                matrix.add(index);
            }
        }

        //Check for index duplication
        checkIndexDuplication(matrix, source, destinationColumns.size());

        //return matrix
        return matrix.toArray(new Integer[matrix.size()]);
    }

}
