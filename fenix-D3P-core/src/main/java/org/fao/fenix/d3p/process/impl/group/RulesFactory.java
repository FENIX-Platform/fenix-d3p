package org.fao.fenix.d3p.process.impl.group;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.Properties;
import org.fao.fenix.d3p.cache.CacheFactory;
import org.fao.fenix.d3p.process.type.RuleName;
import org.fao.fenix.d3s.cache.D3SCache;
import org.fao.fenix.d3s.cache.manager.CacheManager;
import org.fao.fenix.d3s.cache.storage.dataset.DatasetStorage;
import org.reflections.Reflections;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
public class RulesFactory {
    private static Map<String, Map<String,Rule>> parametersMap = new HashMap<>();

    public String setRule(String alias, String pid, Map<String, Object> params, DSDDataset dsd, String ... columns) {
        Rule rule = getInstance(alias);
        String cid = columns!=null && columns.length>0 ? columns[0] : null;
        if (rule != null && cid!=null) {
            Map<String,Rule> rulesByColumn = parametersMap.get(pid);
            if (rulesByColumn==null)
                parametersMap.put(pid, rulesByColumn=new HashMap<>());
            rulesByColumn.put(cid, rule);

            rule.setColumns(columns);
            rule.setConfig(configurationsMap.get(alias));
            rule.setParams(params);
            rule.setDsd(dsd);

            return pid+"@@@"+cid;
        }
        return null;
    }

    public static Rule getRule(String id) {
        int separatorIndex = id!=null ? id.indexOf("@@@") : -1;
        String pid = separatorIndex>0 ? id.substring(0, separatorIndex) : null;
        String cid = separatorIndex>0 ? id.substring(separatorIndex+3) : null;

        Map<String,Rule> rulesByColumn = pid!=null? parametersMap.get(pid) : null;
        return rulesByColumn!=null && cid!=null ? rulesByColumn.get(cid) : null;
    }

    public void delRule(String pid) {
        if (pid!=null)
            parametersMap.remove(pid);
    }

    //INIT
    public void init(String basePackage) throws Exception {
        initInstances(basePackage);
        initConfig();
        registerRules();

    }

    //Config
    private Map<String, Map<String,String>> configurationsMap = new HashMap<>();

    private void initConfig() throws Exception  {
        Properties initParameters = Properties.getInstance(
                "/org/fao/fenix/config/aggregations.properties",
                "file:config/aggregations.properties"
        );

        for (String alias : rulesClass.keySet())
            configurationsMap.put(alias, initParameters.toMap(alias));
    }

    //Factory
    private @Inject Instance<Rule> instances;
    private Map<String, Class<? extends Rule>> rulesClass = new HashMap<>();

    private void initInstances(String basePackage) {
        Reflections reflections = new Reflections(basePackage);
        Set<Class<? extends Rule>> subTypes = reflections.getSubTypesOf(Rule.class);
        if (subTypes!=null)
            for (Class<? extends Rule> ruleClass : subTypes) {
                //Retrieve name
                RuleName annotation = ruleClass.getAnnotation(RuleName.class);
                String name = annotation!=null ? annotation.value() : null;
                name = name==null ? ruleClass.getSimpleName() : name;
                //Maintain the class
                rulesClass.put(name, ruleClass);
            }
    }

    public Rule getInstance(String alias) {
        Class<? extends Rule> ruleClass = rulesClass.get(alias);
        return ruleClass!=null ? instances.select(ruleClass).iterator().next() : null;
    }

    //Custom database functions
    private @Inject CacheFactory cacheFactory;
    private void registerRules() throws Exception  {
        CacheManager<DSDDataset,Object[]> cacheManager = cacheFactory.getDatasetCacheManager(D3SCache.fixed);
        DatasetStorage cacheStorage = cacheManager!=null ? (DatasetStorage)cacheManager.getStorage() : null;
        Connection connection = cacheStorage!=null ? cacheStorage.getConnection() : null;
        if (connection==null)
            throw new UnsupportedOperationException("No cache available");

        try {
            Statement statement = connection.createStatement();
            for (Map.Entry<String, Class<? extends Rule>> rulesClassEntry : rulesClass.entrySet()) {
                statement.addBatch("DROP AGGREGATE IF EXISTS " + rulesClassEntry.getKey());
                statement.addBatch("CREATE AGGREGATE " + rulesClassEntry.getKey() + " FOR \"" + rulesClassEntry.getValue().getName() + '"');
            }
            statement.executeBatch();
        } finally {
            connection.close();
        }

    }


}
