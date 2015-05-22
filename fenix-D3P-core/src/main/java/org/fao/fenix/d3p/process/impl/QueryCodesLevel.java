package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.Aggregation;
import org.fao.fenix.d3p.process.dto.CodesLevel;
import org.fao.fenix.d3p.process.impl.group.RulesFactory;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.msd.services.spi.Resources;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

@ProcessName("group")
public class QueryCodesLevel extends org.fao.fenix.d3p.process.Process<CodesLevel> {
    private @Inject Resources resourcesService;



    private @Inject RulesFactory rulesFactory;
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;
    private @Inject UIDUtils uidUtils;


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
        String customFunctionName = this.getClass().getName()+'.'+this.getClass().getMethod("CODE_TO_LEVEL", String.class, String.class, Integer.class).getName();
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
    public Step process(Connection connection, CodesLevel params, Step... sourceStep) throws Exception {
        if (!registered)
            initCustomFuncion(connection);
        //If the filter is empty return the source
        if (params==null || params.size()==0)
            return sourceStep[0];
        //Retrieve source informations
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("query filter can be applied only on a table or an other select query");
        String sourceData = (String)source.getData();
        if (type==StepType.table)
            sourceData = getCacheStorage().getTableName(sourceData);
        DSDDataset dsd = source.getDsd();
        //Verify code columns correspondence
        Map<String, String> conversionQuerySegments = new HashMap<>();
        for (Map.Entry<String, Integer> param : params.entrySet()) {
            DSDColumn column = dsd.getColumn(param.getKey());
            if (column==null)
                throw new Exception("Undefined column: "+param.getKey());
            if (column.getDataType()!=DataType.code)
                throw new Exception("Selected column isn't a code column");

            OjCodeList domain = column.getDomain().getCodes().iterator().next();
            Map<String, String>[] conversionMap = getLevelConversionMap(domain.getIdCodeList(), domain.getVersion());
            if (conversionMap==null)
                throw new Exception("Required code list is unavailable: "+domain.getIdCodeList()+" - "+domain.getVersion());
            if (param.getValue()>conversionMap.length)
                throw new Exception("Required level ("+param.getValue()+") is unavailable for the selected codelist: "+domain.getIdCodeList()+" - "+domain.getVersion());

            conversionQuerySegments.put(param.getKey(), createConversionQuerySegment(getCodeListId(domain.getIdCodeList(), domain.getVersion()), param.getKey(), param.getValue()));
        }
        //Return correspondent "query" step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(createGroupQuery(params, conversionQuerySegments, dsd, sourceData));
        return step;
    }

    //QUERY UTILS
    private String createConversionQuerySegment(String clid, String columnId, int level) {
        return new StringBuilder("CODE_TO_LEVEL ('")
                .append(clid)
                .append("',")
                .append(columnId)
                .append(',')
                .append(level)
                .append(", false) AS ")
                .append(columnId)
        .toString();
    }

    private String createGroupQuery(CodesLevel params, Map<String, String> conversionQuerySegments, DSDDataset dsd, String source) throws Exception {
        //Prepare select section
        StringBuilder query = new StringBuilder("SELECT ");
        for (DSDColumn column : dsd.getColumns())
            query.append(conversionQuerySegments.containsKey(column.getId()) ? conversionQuerySegments.get(column.getId()) : column.getId()).append(',');
        //Finish query build
        query.setLength(query.length() - 1);
        query.append(" FROM ").append(source);
        return query.toString();
    }


}
