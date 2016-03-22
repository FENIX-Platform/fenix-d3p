package org.fao.fenix.d3p.process.impl.union.logic;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.process.dto.UnionJoin;
import org.fao.fenix.d3p.process.type.UnionLogicName;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

@UnionLogicName("manual")
@ApplicationScoped
public class Manual extends Base {
    private @Inject DatabaseUtils databaseUtils;


    @Override
    public QueryStep[] getUnionQuerySteps(Collection<Collection<Object[]>> sourcesByStorage) throws Exception {
        //Create destination DSD
        Object[] firstSourceInfo = sourcesByStorage.iterator().next().iterator().next();
        DSDDataset firstDatasetDsd = ((Step)firstSourceInfo[0]).getDsd();
        UnionJoin firstSourceJoinInfo = (UnionJoin)firstSourceInfo[1];
        String[] selectedColumns = firstSourceJoinInfo!=null ? firstSourceJoinInfo.getColumns() : null;

        DSDColumn[] destinationColumns = null;
        if (selectedColumns!=null && selectedColumns.length>0) {
            destinationColumns = new DSDColumn[selectedColumns.length];
            for (int i=0; i<selectedColumns.length; i++)
                destinationColumns[i] = selectedColumns[i]!=null ? firstDatasetDsd.findColumn(selectedColumns[i]) : null;
        } else
            destinationColumns = firstDatasetDsd.getColumns().toArray(new DSDColumn[firstDatasetDsd.getColumns().size()]);

        //Create transpose matrix and extend columns domain
        Collection<Collection<Integer[]>> transposeMatrixGroups = new LinkedList<>();
        for (Collection<Object[]> sources : sourcesByStorage) {
            Collection<Integer[]> transposeMatrixGroup = new LinkedList<>();
            for (Object[] sourceInfo : sources) {
                Step source = (Step) sourceInfo[0];
                DSDDataset dsd = source.getDsd();
                UnionJoin joinInfo = (UnionJoin) sourceInfo[1];
                String[] columnsFilter = joinInfo!=null ? joinInfo.getColumns() : null;

                Integer[] matrix = new Integer[destinationColumns.length];
                if(columnsFilter!=null && columnsFilter.length>0) {
                    if (columnsFilter.length!=destinationColumns.length)
                        throw new BadRequestException("Declared destination columns into union filter have different lengths. Source: "+source.getRid().getId());
                    ArrayList<String> columnsFilterList = new ArrayList<>(Arrays.asList(columnsFilter));
                    Set<String> columnsFilterSet = new HashSet<>(Arrays.asList(columnsFilter));
                    //Create matrix from columns filter to DSD
                    int dsdIndex=0;
                    for (DSDColumn column : dsd.getColumns()) {
                        int filterIndex = columnsFilterList.indexOf(column.getId());
                        if (filterIndex>=0) {
                            if (destinationColumns[filterIndex]!=null)
                                extendDomain(source, column, destinationColumns[filterIndex]);
                            else
                                destinationColumns[filterIndex] = column;
                            matrix[dsdIndex] = filterIndex;
                        }
                        dsdIndex++;
                    }
                } else {
                    Collection<DSDColumn> columns = dsd.getColumns();
                    if (columns.size()<destinationColumns.length)
                        throw new BadRequestException("Declared destination columns into union filter have different lengths. Source: "+source.getRid().getId());
                    Iterator<DSDColumn> columnsIterator = columns.iterator();
                    for (int i=0; i<destinationColumns.length; i++)
                        if (destinationColumns[i]!=null)
                            extendDomain(source, columnsIterator.next(), destinationColumns[i]);
                        else
                            destinationColumns[i] = columnsIterator.next();

                    for (int i=0; i<matrix.length; i++)
                        matrix[i] = i;
                }
                transposeMatrixGroup.add(matrix);
            }
            transposeMatrixGroups.add(transposeMatrixGroup);
        }

        //Create resulting query steps
        return createQuerySteps(sourcesByStorage, new ArrayList<>(Arrays.asList(destinationColumns)), transposeMatrixGroups);
    }


    /*
            //Add label columns into parameters if specified
        if (labels) {
            Language[] languages = DatabaseStandards.getLanguageInfo();
            if (languages!=null && languages.length>0) {
                //Retrieve label columns
                Set<String> labelColumns = new HashSet<>();
                for (Collection<Object[]> sources : sourcesByStorage)
                    for (Object[] sourceInfo : sources) {
                        UnionJoin joinInfo = (UnionJoin) sourceInfo[1];
                        String[] columnsFilter = joinInfo != null ? joinInfo.getColumns() : null;
                        if (columnsFilter != null && columnsFilter.length > 0) {
                            Set<String> columnsFilterSet = new LinkedHashSet<>(Arrays.asList(columnsFilter));
                            for (DSDColumn column : ((Step) sourceInfo[0]).getDsd().getColumns())
                                if (columnsFilterSet.contains(column.getId()) && (column.getDataType() == DataType.code || column.getDataType() == DataType.customCode))
                                    for (Language language : languages)
                                        labelColumns.add(column.getId() + '_' + language.getCode());
                        }
                    }
                if (labelColumns.size()>0) {

                }
            }
        }

     */

}
