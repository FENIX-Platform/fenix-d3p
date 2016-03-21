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
import org.fao.fenix.d3p.process.type.UnionUsing;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

public abstract class Base implements Logic {
    private @Inject StepFactory stepFactory;




    protected QueryStep[] createQuerySteps(Collection<Collection<Object[]>> sourcesByStorage, ArrayList<DSDColumn> destinationColumns, Collection<Collection<Integer[]>> transposeMatrixGroups) {
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

                Object[] sourceParameters = source.getType()== StepType.query ? ((QueryStep)source).getParams() : null;
                if (sourceParameters!=null && sourceParameters.length>0)
                    parameters.addAll(Arrays.asList(sourceParameters));

                query.append(createSelect(source, transposeMatrixGroupIterator.next(), destinationColumns.size())).append(" UNION ALL ");
            }

            DSDDataset dsd = new DSDDataset();
            dsd.setContextSystem("D3P");
            dsd.setColumns(destinationColumns);

            QueryStep result = (QueryStep)stepFactory.getInstance(StepType.query);
            result.setStorage(storage);
            result.setDsd(dsd);
            result.setData(query.substring(0, query.length()-" UNION ALL ".length()));
            result.setParams(parameters.toArray());
            result.setTypes(null); //TODO support types
            resultList.add(result);
        }

        return resultList.toArray(new QueryStep[resultList.size()]);
    }

    protected String createSelect(Step source, Integer[] transposeMatrix, int destinationSize) throws UnsupportedOperationException {
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
                query.append(sourceColumnsArray[invertedMatrix[i]].getId()).append(',');
        query.deleteCharAt(query.length()-1);
        query.append(" FROM ").append(tableName);

        return query.toString();
    }


    protected Integer getMatchIndex(DSDColumn column, Step source, Collection<Integer> matrix, int destinationSize, UnionUsing joinMethod, Map<String, Integer> subjectsIndex, Map<String, Integer> idsIndex) throws BadRequestException {
        Integer index = null;

        switch (joinMethod) {
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
                if ((index = matrix.size())>=destinationSize)
                    throw new BadRequestException("Manually specified index out of bound for column " + source.getRid().getId()+'.'+column.getId());
                break;
        }

        return index;
    }



    protected void checkIndexDuplication(Collection<Integer> matrix, Step source, int destinationSize) throws BadRequestException {
        boolean[] inverseMatrix = new boolean[destinationSize];
        for (Integer index : matrix)
            if (index!=null)
                if (inverseMatrix[index])
                    throw new BadRequestException("Multiple column corrispondence for source "+source.getRid().getId());
                else
                    inverseMatrix[index]=true;
    }

    protected void extendDomain (Step source, DSDColumn column, DSDColumn destinationColumn) throws BadRequestException {
        DataType type = column.getDataType();
        if (
                type!=destinationColumn.getDataType() ||
                        (destinationColumn.getDomain()==null && column.getDomain()!=null) ||
                        (destinationColumn.getDomain()!=null && !destinationColumn.getDomain().extend(type,column.getDomain()))
                )
            throw new BadRequestException("Domain confilict for column "+source.getRid().getId()+'.'+column.getId());
    }



}
