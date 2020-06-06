package ink.andromeda.strawberry.core;


import ink.andromeda.strawberry.entity.IndexKey;
import ink.andromeda.strawberry.entity.TableMetaInfo;
import ink.andromeda.strawberry.tools.GeneralTools;
import ink.andromeda.strawberry.tools.Pair;
import ink.andromeda.strawberry.tools.SQLTemplate;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ink.andromeda.strawberry.core.StrawberryService.SIMPLE_TASK_POOL_EXECUTOR;
import static ink.andromeda.strawberry.tools.GeneralTools.*;

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

    public void executeQuery(String sql) {
        StopWatch stopWatch = new StopWatch("execute sql query");

        stopWatch.start("parser sql");
        CrossOriginSQLParser crossOriginSQLParser = new CrossOriginSQLParser(sql);
        stopWatch.stop();

        stopWatch.start("analysis relation");
        VirtualRelation virtualRelation = crossOriginSQLParser.analysis();
        stopWatch.stop();

        stopWatch.start("analysis driving table");
        String drivingTable = analysisDrivingTable(virtualRelation);
        stopWatch.stop();


        log.info(stopWatch.prettyPrint());
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

        public List<Map<String, Object>> getMatchData(String indexName, IndexKey indexKey) {
            return Objects.requireNonNull(index.get(indexName)).get(indexKey);
        }

        public void addIndexData(String indexName, IndexKey indexKey, Map<String, Object> data) {

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

    /**
     * 找出驱动表, 即从哪一张表开始查询
     *
     * @param relation 表连接关系
     * @return 驱动表的表明(别名)
     * @throws IllegalStateException 无法找到驱动表
     */
    private String analysisDrivingTable(VirtualRelation relation) {
        CountDownLatch countDownLatch = new CountDownLatch(relation.getWhereCases().size());
        TreeSet<Pair<String, Long>> record = new TreeSet<>(Comparator.comparingLong(Pair::getRight));
        // 遍历所有表的条件组, 多线程方式
        relation.getWhereCases().forEach((k, v) -> {
            SIMPLE_TASK_POOL_EXECUTOR.submit(() -> {
                try {
                    // 获取表的全名
                    String tableFullName = relation.getTableLabelRef().get(k);
                    // 获取数据源名称
                    String sourceName = tableFullName.substring(0, tableFullName.indexOf('.'));
                    // 获取出去数据源名称前缀的表名
                    String tableName = tableFullName.substring(tableFullName.indexOf('.') + 1);
                    // 获取数据源实例
                    DataSource dataSource = getNonNullDataSource(sourceName);
                    // 获取查询时的sql where条件语句
                    String sqlCondition = toSQLCondition(k, tableName, v);
                    // 生成explain SQL
                    String explainSQL = SQLTemplate.explainResultSql(tableName, sqlCondition);
                    log.info(explainSQL);
                    Map<String, Object> result = simpleQueryOne(dataSource, explainSQL);
                    if (result != null) {
                        Long rows = conversionService().convert(result.get("rows"), Long.class);
                        record.add(Pair.of(k, rows));
                    }
                } catch (Exception ex) {
                    log.error("error in analysis driving table: {}", ex.toString(), ex);
                } finally {
                    countDownLatch.countDown();
                }
            });
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.error(e.toString(), e);
        }
        if (record.size() == 0) {
            throw new IllegalStateException("could not find minimum scan rows for relation: " + relation);
        }
        // 扫描行数最小的即为驱动表
        String result = record.last().getLeft();
        log.info("find driving table: {}, may scan rows: {}", result, record.last().getValue());
        return result;
    }

    private DataSource getNonNullDataSource(String sourceName) {
        return Objects.requireNonNull(this.dataSourceMap.get(sourceName), "data source '" + sourceName + "' is null!");
    }

    private static String toSQLCondition(String tableLabelName, String correctTableName, List<String> whereCases) {
        return whereCases.stream().map(c -> c.replaceAll("^\\s*\\b" + tableLabelName + "\\b", correctTableName))
                .collect(Collectors.joining(" AND ", " ", " "));
    }
}
