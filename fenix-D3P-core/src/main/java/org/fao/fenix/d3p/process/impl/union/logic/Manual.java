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
    public QueryStep[] getUnionQuerySteps(Collection<Collection<Object[]>> sourcesByStorage, boolean labels) throws Exception {

        //Add label columns into parameters if specified
        if (labels) {
            Language[] languages = DatabaseStandards.getLanguageInfo();
            if (languages!=null && languages.length>0)
                for (Collection<Object[]> sources : sourcesByStorage)
                    for (Object[] sourceInfo : sources) {
                        UnionJoin joinInfo = (UnionJoin) sourceInfo[1];
                        String[] columnsFilter = joinInfo!=null ? joinInfo.getColumns() : null;
                        if (columnsFilter != null && columnsFilter.length > 0) {
                            Set<String> columnsFilterSet = new LinkedHashSet<>(Arrays.asList(columnsFilter));
                            for (DSDColumn column : ((Step) sourceInfo[0]).getDsd().getColumns())
                                if (column.getDataType() == DataType.code || column.getDataType() == DataType.customCode)
                                    for (Language language : languages)
                                        columnsFilterSet.add(column.getId() + '_' + language.getCode());
                            joinInfo.setColumns(columnsFilterSet.toArray(new String[columnsFilterSet.size()]));
                        }
                    }
        }

        //Create destination DSD
        ArrayList<DSDColumn> destinationColumns = new ArrayList<>(((Step)sourcesByStorage.iterator().next().iterator().next()[0]).getDsd().getColumns());

        //Create transpose matrix and extend columns domain
        Collection<Collection<Integer[]>> transposeMatrixGroups = new LinkedList<>();
        for (Collection<Object[]> sources : sourcesByStorage) {
            Collection<Integer[]> transposeMatrixGroup = new LinkedList<>();
            for (Object[] sourceInfo : sources) {
                Step source = (Step) sourceInfo[0];
                DSDDataset dsd = source.getDsd();
                UnionJoin joinInfo = (UnionJoin) sourceInfo[1];
                String[] columnsFilter = joinInfo!=null ? joinInfo.getColumns() : null;

                Integer[] matrix = new Integer[destinationColumns.size()];
                if(columnsFilter!=null && columnsFilter.length>0) {
                    if (columnsFilter.length!=destinationColumns.size())
                        throw new BadRequestException("Declared destination columns into union filter have different lengths. Source: "+source.getRid().getId());
                    Set<String> columnsFilterSet = new HashSet<>(Arrays.asList(columnsFilter));
                    int i=0, index=0;
                    for (DSDColumn column : dsd.getColumns()) {
                        if (columnsFilterSet.contains(column.getId())) {
                            extendDomain(source, column, destinationColumns.get(index));
                            matrix[index++] = i;
                        }
                        i++;
                    }
                    if (index<destinationColumns.size())
                        throw new BadRequestException("Wrong columns name for union filter. Source: "+source.getRid().getId());
                } else {
                    Collection<DSDColumn> columns = dsd.getColumns();
                    if (columns.size()<destinationColumns.size())
                        throw new BadRequestException("Declared destination columns into union filter have different lengths. Source: "+source.getRid().getId());
                    Iterator<DSDColumn> columnsIterator = columns.iterator();
                    for (DSDColumn destinationColumn : destinationColumns)
                        extendDomain(source, columnsIterator.next(), destinationColumn);

                    for (int i=0; i<matrix.length; i++)
                        matrix[i] = i;
                }
                transposeMatrixGroup.add(matrix);
            }
            transposeMatrixGroups.add(transposeMatrixGroup);
        }

        //Create resulting query steps
        return createQuerySteps(sourcesByStorage, destinationColumns, transposeMatrixGroups);
    }

}
