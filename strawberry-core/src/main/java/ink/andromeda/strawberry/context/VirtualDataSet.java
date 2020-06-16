package ink.andromeda.strawberry.context;

import ink.andromeda.strawberry.core.*;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

import static ink.andromeda.strawberry.core.CrossSourceSQLParser.*;

/**
 * 虚拟数据集
 */
public class VirtualDataSet {

    @Getter
    private final String name;

    private final CrossSourceQueryEngine queryEngine;

    private final LinkRelation linkRelation;

    public VirtualDataSet(QueryEnvironmentProvider provider, String name, String sql) {
        sql = checkSQL(sql);
        this.name = name;
        linkRelation = analysisRelation(sql);
        this.queryEngine = new CrossSourceQueryEngine(provider::getTableMetaInfo, provider::getDataSource);
    }

    public VirtualDataSet(QueryEnvironmentProvider provider, String name, List<String> sql) {
        List<String> outputFields = new ArrayList<>();
        sql.forEach(s -> {
            s = checkSQL(s);
            LinkRelation linkRelation = analysisRelation(s);
            if(outputFields.isEmpty()) {
                outputFields.addAll(linkRelation.getOutputFieldLabels());
                return;
            }
            if(outputFields.equals(linkRelation.getOutputFieldLabels())){
                throw new IllegalArgumentException("the sql has no same output field list");
            };
        });
        this.name = name;
        linkRelation = new LinkRelation();
        this.queryEngine = new CrossSourceQueryEngine(provider::getTableMetaInfo, provider::getDataSource);
    }

    public QueryResults executeQuery(String querySQL) throws Exception {
        QueryCondition condition = analysisConditionFromWhereClause(querySQL);
        return queryEngine.executeQuery(linkRelation, condition);
    }

    public QueryResults executeQuery(QueryCondition condition) throws Exception {
        return queryEngine.executeQuery(linkRelation, condition);
    }
}
