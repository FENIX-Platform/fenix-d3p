package org.fao.fenix.d3p.process.impl.union.logic;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.UnionJoin;
import org.fao.fenix.d3p.process.impl.union.Logic;
import org.fao.fenix.d3p.process.type.UnionLogicName;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;
import java.util.Iterator;

@UnionLogicName("extend")
@ApplicationScoped
public class Extend implements Logic {
    private @Inject StepFactory stepFactory;


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
        Collection<QueryStep> resultList = new LinkedList<>();

        Iterator<Collection<Integer[]>> transposeMatrixGroupsIterator = transposeMatrixGroups.iterator();
        for (Collection<Object[]> sources : sourcesByStorage) {
            Iterator<Integer[]> transposeMatrixGroupIterator = transposeMatrixGroupsIterator.next().iterator();

            DatasetStorage storage = null;
            Collection<Object> parameters = new LinkedList<>();
            StringBuilder query = new StringBuilder();

            for (Object[] sourceInfo : sources) {
                Step source = (Step)sourceInfo[0];
                if (storage==null)
                    storage = source.getStorage();

                Object[] sourceParameters = source.getType()==StepType.table ? ((QueryStep)source).getParams() : null;
                if (sourceParameters!=null && sourceParameters.length>0)
                    parameters.addAll(Arrays.asList(sourceParameters));

                query.append(createSelect(source, transposeMatrixGroupIterator.next(), destinationColumns.size()));
            }

            DSDDataset dsd = new DSDDataset();
            dsd.setContextSystem("D3P");
            dsd.setColumns(destinationColumns);

            QueryStep result = (QueryStep)stepFactory.getInstance(StepType.query);
            result.setStorage(storage);
            result.setDsd(dsd);
            result.setData(query.toString());
            result.setParams(parameters.toArray());
            result.setTypes(null); //TODO support types
            resultList.add(result);
        }

        return resultList.toArray(new QueryStep[resultList.size()]);
    }

    private String createSelect(Step source, Integer[] transposeMatrix, int destinationSize) throws UnsupportedOperationException {
        //Retrieve source info
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("filter union can be applied only on a table or query sources");
        String tableName = type==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        //Prepare conversion matrix
        Integer[] invertedMatrix = new Integer[destinationSize];
        for (int i=0; i<transposeMatrix.length; i++)
            invertedMatrix[transposeMatrix[i]] = i;
        DSDColumn[] sourceColumnsArray = source.getDsd().getColumns().toArray(new DSDColumn[source.getDsd().getColumns().size()]);
        //Return query
        StringBuilder query = new StringBuilder("SELECT ");
        for (int i=0; i<invertedMatrix.length; i++)
            if (invertedMatrix[i]==null)
                query.append("NULL AS column_").append(i).append(',');
            else
                query.append(sourceColumnsArray[invertedMatrix[i]]).append(',');
        query.deleteCharAt(query.length()-1);
        query.append(" FROM ").append(tableName);

        return query.toString();
    }

    private Integer[] getTransposeMatrix(ArrayList<DSDColumn> destinationColumns, Step source, UnionJoin joinType, Map<String, Integer> subjectsIndex, Map<String, Integer> idsIndex) {
        Collection<Integer> matrix = new LinkedList<>();
        List<String> joinColumns = joinType.getColumns()!=null && joinType.getColumns().length>0 ? Arrays.asList(joinType.getColumns()) : null;

        for (DSDColumn column : source.getDsd().getColumns()) {
            if (joinColumns==null || joinColumns.contains(column.getId())) {
                Integer index = null;
                column.setKey(false);

                switch (joinType.getUsing()) {
                    case idOnly:
                        if ((index = idsIndex.get(column.getId())) == null && subjectsIndex.containsKey(column.getSubject()))
                            column.setSubject(null);
                        break;
                    case subjectOnly:
                        if ((index = subjectsIndex.get(column.getSubject())) == null && idsIndex.containsKey(column.getId()))
                            column.setId(column.getId() + '_' + source.getRid().getId());
                        break;
                    case idFirst:
                        if ((index = idsIndex.get(column.getId())) == null)
                            index = subjectsIndex.get(column.getSubject());
                        break;
                    case subjectFirst:
                        if ((index = subjectsIndex.get(column.getSubject())) == null)
                            index = idsIndex.get(column.getId());
                        break;
                    case index:
                        index = matrix.size();
                        break;
                }
                //add as a new column or check and extend existing column domain
                if (index==null)
                    index = destinationColumns.size();
                if (index==destinationColumns.size()) {
                    destinationColumns.add(column);
                    idsIndex.put(column.getId(), index);
                    if (column.getSubject()!=null)
                        subjectsIndex.put(column.getSubject(), index);
                } else {
                    DSDColumn destinationColumn = destinationColumns.get(index);
                    DataType type = column.getDataType();
                    if (
                            column.getDataType()!=destinationColumn.getDataType() ||
                            (destinationColumn.getDomain()==null && column.getDomain()!=null) ||
                            (destinationColumn.getDomain()!=null && !destinationColumn.getDomain().extend(type,column.getDomain()))
                            )
                        throw new BadRequestException("Domain confilict for column "+source.getRid().getId()+'.'+column.getId());
                }
                //update transpose data
                matrix.add(index);
            }
        }

        return matrix.toArray(new Integer[matrix.size()]);
    }

}
