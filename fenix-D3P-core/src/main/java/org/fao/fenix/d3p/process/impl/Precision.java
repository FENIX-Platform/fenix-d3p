package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.PrecisionLevel;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;
import org.fao.fenix.d3s.cache.storage.dataset.h2.DefaultStorage;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

@ProcessName("precision")
public class Precision extends org.fao.fenix.d3p.process.Process<PrecisionLevel> {
    private @Inject Resources resourcesService;
    private @Inject StepFactory stepFactory;


    //H2 custom function
    private static boolean registered = false;

    public static String CODE_TO_LEVEL(String codeList, String code, Integer level, Boolean nullable) throws Exception {
        String newCode = code!=null ? levelConversionCodelists.get(codeList)[level].get(code) : null;
        if (newCode==null && !nullable && code!=null)
            throw new Exception("Upper level code not found for code: "+codeList+" - "+code);
        return newCode;
    }

    public void initCustomFuncion(Connection connection) throws Exception {
        //Init custom function
        Statement statement = connection.createStatement();
        String customFunctionName = this.getClass().getName()+".CODE_TO_LEVEL";
        statement.addBatch("DROP ALIAS IF EXISTS CODE_TO_LEVEL");
        statement.addBatch("CREATE ALIAS CODE_TO_LEVEL FOR \""+customFunctionName+"\"");
        statement.executeBatch();
        registered = true;
    }


    //Local in-memory cache
    private static final Map<String, Map<String, String>[]> levelConversionCodelists = new HashMap<>();
    private static final Map<String, Date> codelistsLastUpdate = new HashMap<>();

    private Map<String, String>[] getLevelConversionMap(String uid, String version) throws Exception {
        //Load codelist
        MeIdentification codelistMetadata = resourcesService.loadMetadata(uid, version);
        if (codelistMetadata==null)
            throw new Exception("Undefined codelist: "+uid+(version!=null ? " - "+version : ""));
        //Retrieve meta information
        String clid = getCodeListId(uid, version);
        Map<String, String>[] conversionMap = levelConversionCodelists.get(clid);
        Date lastUpdateDate = getLastUpdateDate(codelistMetadata);
        Date currentLastUpdateDate = codelistsLastUpdate.get(clid);
        //Load (if needed) and return conversion map
        if (conversionMap==null || (lastUpdateDate!=null && (currentLastUpdateDate==null || currentLastUpdateDate.before(lastUpdateDate)))) {
            levelConversionCodelists.put(clid, conversionMap = createConversionMap(uid, version));
            codelistsLastUpdate.put(clid, lastUpdateDate);
        }
        return conversionMap;
    }

    private Date getLastUpdateDate(MeIdentification metadata) throws Exception {
        MeMaintenance meMaintenance = metadata!=null ? metadata.getMeMaintenance() : null;
        SeUpdate seUpdate = meMaintenance!=null ? meMaintenance.getSeUpdate() : null;
        return seUpdate!=null ? seUpdate.getUpdateDate() : null;
    }

    private Map<String, String>[] createConversionMap(String uid, String version) throws  Exception {
        Resource<DSDCodelist, Code> codelistResource = resourcesService.loadResource(uid, version);
        Collection<Code> codes = codelistResource.getData();
        ArrayList<Map<String, String>> conversionMap = new ArrayList<>();

        Stack<String> parents = new Stack<>();
        if (codes!=null)
            for (Code code : codes)
                appendToConversionMap(parents, code, conversionMap, 0);

        return conversionMap.toArray(new Map[conversionMap.size()]);
    }

    private void appendToConversionMap(Stack<String> parents, Code codeObject, ArrayList<Map<String, String>> conversionMap, int level) {
        //Add parents levels conversion
        String code = codeObject.getCode();
        Iterator<Map<String, String>> levelMapIterator = conversionMap.iterator();
        for (String parent : parents)
            levelMapIterator.next().put(code, parent);
        //Add current level conversion
        Map<String, String> levelMap = conversionMap.size()>level ? conversionMap.get(level) : null;
        if (levelMap==null)
            conversionMap.add(levelMap = new HashMap<>());
        levelMap.put(code,code);
        //Add next levels conversion
        Collection<Code> children = codeObject.getChildren();
        if (children!=null) {
            parents.push(code);
            for (Code child : children)
                appendToConversionMap(parents, child, conversionMap, level+1);
            parents.pop();
        }
    }

    private String getCodeListId (String uid, String version) {
        return version!=null ? uid+'|'+version : uid;
    }


    //PROCESS LOGIC
    @Override
    public Step process(PrecisionLevel params, Step... sourceStep) throws Exception {
        //Retrieve source informations
        Step source = sourceStep!=null && sourceStep.length>0 ? sourceStep[0] : null;
        StepType sourceType = source!=null ? source.getType() : null;
        if (sourceType==null || (sourceType!=StepType.table && sourceType!=StepType.query))
            throw new UnsupportedOperationException("Precision filter can be applied only on a table or an other select query");
        String tableName = sourceType==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();
        //Register custom function to H2 storage
        if (!registered) {
            DatasetStorage cacheStorage = source.getStorage();
            if (cacheStorage instanceof DefaultStorage) {
                Connection connection = cacheStorage.getConnection();
                try {
                    initCustomFuncion(connection);
                } finally {
                    connection.close();
                }
            }
        }
        //Verify code columns correspondence
        Map<String, String> conversionQuerySegments = new HashMap<>();
        for (Map.Entry<String, Integer> param : params.entrySet()) {
            DSDColumn column = dsd.findColumn(param.getKey());
            if (column==null)
                throw new Exception("Undefined column: "+param.getKey());
            if (column.getDataType()==DataType.code) { //Coded column
                OjCodeList domain = column.getDomain().getCodes().iterator().next();
                Map<String, String>[] conversionMap = getLevelConversionMap(domain.getIdCodeList(), domain.getVersion());
                if (conversionMap==null)
                    throw new Exception("Required code list is unavailable: "+domain.getIdCodeList()+" - "+domain.getVersion());
                if (param.getValue()>conversionMap.length)
                    throw new Exception("Required level ("+param.getValue()+") is unavailable for the selected codelist: "+domain.getIdCodeList()+" - "+domain.getVersion());

                conversionQuerySegments.put(param.getKey(), createConversionQuerySegment(getCodeListId(domain.getIdCodeList(), domain.getVersion()), param.getKey(), param.getValue()));
            }else //Time column
                conversionQuerySegments.put(param.getKey(), createConversionQuerySegment(column, param.getValue()));
        }
        //Update dsd
        updateDsd(dsd,params);
        //Create query
        String query = createGroupQuery(params, conversionQuerySegments, dsd, tableName);
        //Create query step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(query);
        if (sourceType==StepType.query) {
            step.setParams(((QueryStep) source).getParams());
            step.setTypes(((QueryStep) source).getTypes());
        }
        return step;
    }


    //DSD adjustment (not distinct values)
    private void updateDsd(DSDDataset dsd, PrecisionLevel params) throws Exception {
        for (DSDColumn column : dsd.getColumns()) {
            column.setKey(false);
            if (params.containsKey(column.getId()) && column.getDataType()!=DataType.code) {
                long divider = getDivider(column.getDataType(), params.get(column.getId()));
                if (divider>1) {
                    switch (params.get(column.getId())) {
                        case 0: column.setDataType(DataType.year); break;
                        case 1: column.setDataType(DataType.month); break;
                        case 2: column.setDataType(DataType.date); break;
                        case 3: column.setDataType(DataType.time); break;
                    }
                    DSDDomain domain = column.getDomain();
                    Period period = domain!=null ? domain.getPeriod() : null;
                    Long from = period!=null ? period.getFrom() : null;
                    Long to = period!=null ? period.getTo() : null;
                    Collection<Long> timeList = domain!=null ? domain.getTimeList() : null;
                    if (from!=null)
                        period.setFrom(from/divider);
                    if (to!=null)
                        period.setTo(to/divider);
                    if (timeList!=null) {
                        Collection<Long> newTimeList = new LinkedList<>();
                        for (Long time : timeList)
                            newTimeList.add(time/divider);
                        domain.setTimeList(newTimeList);
                    }
                }
            }
        }
    }


    //QUERY UTILS
    private String createConversionQuerySegment(String clid, String columnId, int level) {
        return new StringBuilder("CODE_TO_LEVEL ('")
                .append(clid)
                .append("',")
                .append(columnId)
                .append(',')
                .append(level)
                .append(", true) AS ")
                .append(columnId)
        .toString();
    }


    private String createConversionQuerySegment(DSDColumn column, int level) throws Exception {
        long divider = getDivider(column.getDataType(), level);
        if (divider==1)
            return column.getId();

        return new StringBuilder(column.getId())
                .append('/')
                .append(divider)
                .append(" AS ")
                .append(column.getId())
        .toString();
    }

    private String createGroupQuery(PrecisionLevel params, Map<String, String> conversionQuerySegments, DSDDataset dsd, String source) throws Exception {
        //Prepare select section
        StringBuilder query = new StringBuilder("SELECT ");
        for (DSDColumn column : dsd.getColumns())
            query.append(conversionQuerySegments.containsKey(column.getId()) ? conversionQuerySegments.get(column.getId()) : column.getId()).append(',');
        //Finish query build
        query.setLength(query.length() - 1);
        query.append(" FROM ").append(source);
        return query.toString();
    }

    private long getDivider (DataType type, int level) throws Exception {
        Integer currentLevel = null;
        switch (type) {
            case year: currentLevel = 0; break;
            case month: currentLevel = 1; break;
            case date: currentLevel = 2; break;
            case time: currentLevel = 3; break;
            default:
                throw new Exception("Column data type incompatible");
        }
        if (currentLevel==null)
            throw new Exception("Column data type incompatible");
        if (currentLevel<level)
            throw new Exception("Column precision incompatible with the required level: "+currentLevel+" -> "+level);
        if (currentLevel==level)
            return 1;

        long divider = 1;
        switch (currentLevel) {
            case 1: divider = 100l; break;
            case 2: divider = level==0 ? 10000l : 100l; break;
            case 3: switch (level) {
                case 0: divider = 10000000000l; break;
                case 1: divider = 100000000l; break;
                case 2: divider = 1000000l; break;
            }
        }

        return divider;
    }

}
