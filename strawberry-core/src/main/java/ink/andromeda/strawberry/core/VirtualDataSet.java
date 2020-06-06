package ink.andromeda.strawberry.core;


import ink.andromeda.strawberry.entity.IndexKey;
import ink.andromeda.strawberry.entity.TableMetaInfo;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.*;
import java.util.stream.Stream;

/**
 * 数据集
 */
@Slf4j
public class VirtualDataSet {

    /**
     * 数据集的id
     */
    @Getter
    private final long id;

    /**
     * 数据集的名称(唯一)
     */
    @Getter
    private final String name;

    /**
     * 该数据集所包含的表
     * <p>库名.表名<->表信息</p>
     */
    @Getter
    private final Map<String, TableMetaInfo> originalTables;

    /**
     * 本数据集所包含的原始表的虚拟关系
     */
    private final VirtualRelation virtualRelation;

    /**
     * 数据源集合
     */
    @Setter
    private Map<Object, DataSource> dataSourceMap;

    /**
     * <p>首轮查询时单次最大查询数量, 默认为1000。
     * <p>该值较小时, 单轮连接的计算量小, 但是查询数据库次数较多, 较大时反之, 需要根据limit
     * 和预估结果合理设置。
     */
    @Setter
    private int baseQueryCount = DEFAULT_BASE_QUERY_COUNT;

    private static final int DEFAULT_BASE_QUERY_COUNT = 1000;

    public VirtualDataSet(long id,
                          String name,
                          Map<String, TableMetaInfo> originalTables,
                          VirtualRelation virtualRelation) {
        this.id = id;
        this.name = name;
        this.originalTables = originalTables;
        this.virtualRelation = virtualRelation;
    }

    public VirtualDataSet() {}

    public void executeQuery(String sql){
        CrossOriginSQLParser crossOriginSQLParser = new CrossOriginSQLParser(sql);
        VirtualRelation virtualRelation = crossOriginSQLParser.analysis();
        analysisDrivingTable(virtualRelation);

    }

    /**
     * 执行数据连接操作
     *
     * @return 连接完成的数据
     */
    private List<Map<String, Object>> joining() {

        /*
         * next(right) join:
         *
         *
         * current joined data <=========[left fields<-->right fields]=========> right table (waiting join)
         *
         *
         *      def: result
         *
         *
         * ------------->----------------------------> currentResultDat >--------------iterate data------------------------
         * |                                                                                                              |
         * |     iteration variable of currentResultData: cd                                                              |
         * |                                                                                                              |
         * |                                query waitingJoinedResultSet.joinIndexMap                                     |
         * |                                                   |                                                          |
         * |                                                   |                                                          |
         * |                                                 query                                                        V
         * |                                                   |                                                          |
         * |                                                   V                                                          |
         * ^                    ---------------------------------------------------------------                           |
         * |                    | indexName: right fields                                     |                           |
         * |                    | indexKey:  value of the corresponding left fields(from cd)  |                           |
         * |                    ---------------------------------------------------------------                           |
         * |                                                   |                                                          V
         * |                                                   |                                                          |
         * |                                                   V                                                          |
         * |                         --------------------can joined data------------------                                |
         * |                         |                                                   |                                |
         * |                         V                                                   V                                |
         * |          size is 0 and has no query param                           size is 1 or more:                       |
         * ^                         |                                                   |                                V
         * |                         V                                                   V                                |
         * |                  -------------------                     ---------------------------------------             |
         * |                  |  result add cd  |                     |  joined data.forEach: jd ->         |             |
         * |                  -------------------                     |       result add (cd combine jd)    |             |
         * |                                                          ---------------------------------------             |
         * |                                                                                                              |
         * -----------------------<----------------------<--------------------------<-----------------------<--------------
         *
         *
         * prev(left) join opposite to above
         *
         */

        return null;
    }


    /**
     * 原始表的查询结果
     */
    private static class OriginalResultSet {

        private final String name;

        private Map<String, Map<IndexKey, List<Map<String, Object>>>> index = new HashMap<>(4);

        public List<Map<String, Object>> getMatchData(String indexName, IndexKey indexKey){
            return Objects.requireNonNull(index.get(indexName)).get(indexKey);
        }

        public void addIndexData(String indexName, IndexKey indexKey, Map<String, Object> data){

        }

        @Setter
        @Getter
        private List<Map<String, Object>> data;

        private OriginalResultSet(String name) {
            this.name = name;
        }
    }



    private String[] distinctIndexName(String[] index) {
        return Stream.of(index).distinct().toArray(String[]::new);
    }

    private String[][] distinctIndexName(String[][] compositeIndex) {
        // String[]比较时不会比较内部的元素, 将其连接为字符串去重后再还原
        return Stream.of(compositeIndex)
                .map(s -> String.join("::", s))
                .distinct()
                .map(s -> s.split("::"))
                .toArray(String[][]::new);
    }

    private void buildIndex(String[] index,
                            Map<String, Map<Object, List<Map<String, Object>>>> indexingMap,
                            Map<String, Object> object) {
        for (String iFields : index)
            indexingMap.computeIfAbsent(iFields, k -> new HashMap<>())
                    .computeIfAbsent(object.get(iFields), k -> new ArrayList<>(8)).add(object);
    }

    private void buildIndex(String[][] compositeIndex,
                            Map<String, Map<IndexKey, List<Map<String, Object>>>> compositeIndexMap,
                            Map<String, Object> object) {
        for (String[] index : compositeIndex) {
            Object[] val = Stream.of(index).map(object::get).toArray();
            String indexName = String.join("::", index);
            IndexKey indexKey = IndexKey.of(val);
            compositeIndexMap.computeIfAbsent(indexName, k -> new HashMap<>(16))
                    .computeIfAbsent(indexKey, k -> new ArrayList<>(32)).add(object);
        }
    }

    private String analysisDrivingTable(VirtualRelation relation){

        relation.getTableLabelRef().forEach((k, v) -> {
            String sourceName = v.substring(0, v.indexOf('.'));
            String tableName = v.substring(v.indexOf('.') + 1);

        });

        return null;
    }

}
