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
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    public void executeQuery(String sql) throws Exception{
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

        Map<String, VirtualRelation.VirtualNode> virtualNodeMap = virtualRelation.getVirtualNodeMap();
        Set<String> remainTables = virtualNodeMap.keySet();

        stopWatch.start("execute");
        List<Map<String, Object>> drivingData = getDrivingData(drivingTable, virtualRelation);
        remainTables.remove(drivingTable);


        executeQuery(drivingTable, drivingData, virtualNodeMap, virtualRelation, remainTables);
        stopWatch.stop();

        log.info(stopWatch.prettyPrint());
    }


    private void executeQuery(String currentTable,
                              List<Map<String, Object>> currentData,
                              Map<String, VirtualRelation.VirtualNode> virtualNodeMap,
                              VirtualRelation relation,
                              Set<String> remainTables){



    }

    private List<Map<String, Object>> getDrivingData(String tableLabelName, VirtualRelation relation) {
        String tableFullName = relation.getTableLabelRef().get(tableLabelName);
        String[] splitTableFullName = splitTableFullName(tableFullName);
        String source = splitTableFullName[0];
        String schema = splitTableFullName[1];
        String tableName = splitTableFullName[2];

        DataSource dataSource = getNonNullDataSource(source);
        StringBuilder SQL = new StringBuilder("SELECT * FROM ").append(schema).append(".").append(tableName).append(" WHERE ");
        List<String> wheres = Objects.requireNonNull(relation.getWhereCases().get(tableLabelName));
        String conditions = toSQLCondition(tableLabelName, tableName, wheres);
        SQL.append(conditions);
        log.info(SQL.toString());
        VirtualRelation.VirtualNode node = relation.getVirtualNodeMap().get(tableLabelName);

        /*
        * 获取索引字段
        *
        *
        *                   index fields is -------------------------and---------------------
        *                                              |                                    |
        *                                              V                                    V
        *                                        ----------------                    ---------------
        *                                        |              |                    |             |
        * prev_table0 (left_fields)<--------------(right_fields)-----Current Node-----(left_fields)-------------->(right_fields) next_table0
        *                                        |              |    |         |     |             |
        *                                        |              |    |         |     |             |
        * prev_table1 (left_fields)<--------------(right_fields)------         -------(left_fields)-------------->(right_fields) next_table1
        *                                        |              |                    |             |
        *                      .                 |              |                    |             |        .
        *                      .                 |              |                    |             |        .
        *                      .                 |              |                    |             |        .
        *                  (or more)             |              |                    |             |    (or more)
        *                                        ----------------                    ---------------
        *
        *
        * */
        String[][] index = Stream.concat(node.prev().values().stream().map(s -> s.stream().map(Pair::getRight)),
                node.next().values().stream().map(s -> s.stream().map(Pair::getLeft)))
                .map(s -> s.toArray(String[]::new))
                .toArray(String[][]::new);
        // 索引去重
        index = distinctIndexName(index);
        log.info(Arrays.deepToString(index));
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Map<IndexKey, List<Map<String, Object>>>> indexMap = new HashMap<>(index.length);
        try(
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(SQL.toString());
                ResultSet resultSet = statement.executeQuery();
                ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            String[] labelNames = new String[columnCount + 1];
            for (int i = 1; i <= columnCount; i++) {
                labelNames[i] = tableLabelName + "." + metaData.getColumnName(i);
            }

            while (resultSet.next()){
                Map<String, Object> object = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    object.put(labelNames[i], resultSet.getObject(i));
                }
                buildIndex(index, indexMap, object);
                result.add(object);
            }
        }catch (Exception ex){
            throw new IllegalStateException("exception in execute sql: " + SQL, ex);
        }

        return simpleQuery(dataSource, SQL.toString());
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

        private final String[][] indexField;

        private final Map<String, Map<IndexKey, List<Map<String, Object>>>> index = new HashMap<>(4);

        private final List<Map<String,Object>> data = new ArrayList<>(32);

        public List<Map<String, Object>> getMatchData(String indexName, IndexKey indexKey) {
            return Objects.requireNonNull(index.get(indexName)).get(indexKey);
        }

        public void add(Map<String, Object> object){
            this.data.add(object);
            buildIndex(indexField, index, object);
        }

        private OriginalResultSet(String name, String[][] indexField) {
            this.name = name;
            this.indexField = distinctIndexName(indexField);
        }
    }


    private String[] distinctIndexName(String[] index) {
        return Stream.of(index).distinct().toArray(String[]::new);
    }

    private static String[][] distinctIndexName(String[][] compositeIndex) {
        // String[]比较时不会比较内部的元素, 将其连接为字符串去重后再还原
        return Stream.of(compositeIndex)
                .map(s -> String.join(":", s))
                .distinct()
                .map(s -> s.split(":"))
                .toArray(String[][]::new);
    }

    private void buildIndex(String[] index,
                            Map<String, Map<Object, List<Map<String, Object>>>> indexingMap,
                            Map<String, Object> object) {
        for (String iFields : index)
            indexingMap.computeIfAbsent(iFields, k -> new HashMap<>())
                    .computeIfAbsent(object.get(iFields), k -> new ArrayList<>(8)).add(object);
    }

    private static void buildIndex(String[][] compositeIndex,
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

        // 会有多线程竞争的bug, TreeSet非线程安全
        // TreeSet<Pair<String, Long>> record = new TreeSet<>(Comparator.comparingLong(Pair::getRight));

        Map<Long, String> record = new ConcurrentHashMap<>(relation.getWhereCases().size());

        // 遍历所有表的条件组, 多线程方式
        relation.getWhereCases().forEach((k, v) -> {
            SIMPLE_TASK_POOL_EXECUTOR.submit(() -> {
                try {
                    // 获取表的全名
                    String tableFullName = relation.getTableLabelRef().get(k);

                    String[] splitTableFullName = splitTableFullName(tableFullName);
                    String sourceName = splitTableFullName[0];
                    String schemaName = splitTableFullName[1];
                    String tableName = splitTableFullName[2];
                    // 获取数据源实例
                    DataSource dataSource = getNonNullDataSource(sourceName);
                    // 获取查询时的sql where条件语句
                    String sqlCondition = toSQLCondition(k, tableName, v);
                    // 生成explain SQL
                    String explainSQL = SQLTemplate.explainResultSql(schemaName + "." + tableName, sqlCondition);
                    log.info(explainSQL);
                    Map<String, Object> result = simpleQueryOne(dataSource, explainSQL);
                    if (result != null) {
                        Long rows = conversionService().convert(result.get("rows"), Long.class);
                        record.put(rows, k);
                        // record.add(Pair.of(k, rows));
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
        // 扫描行数最小的即为驱动表
        long minRows = Long.MAX_VALUE;
        for (long l : record.keySet()){
            if(l < minRows) {
                minRows = l;
            }
        }
        if (minRows == Long.MAX_VALUE) {
            throw new IllegalStateException("could not find minimum scan rows for relation: " + relation);
        }

        String result = record.get(minRows);
        log.info("find driving table: {}, may scan rows: {}", result, minRows);
        return result;
    }

    private DataSource getNonNullDataSource(String sourceName) {
        return Objects.requireNonNull(this.dataSourceMap.get(sourceName), "data source '" + sourceName + "' is null!");
    }

    private static String toSQLCondition(String tableLabelName, String correctTableName, List<String> whereCases) {
        return whereCases.stream().map(c -> c.replaceAll("^\\s*\\b" + tableLabelName + "\\b", correctTableName))
                .collect(Collectors.joining(" AND ", " ", " "));
    }

    private String[] splitTableFullName(String tableFullName){
        String[] strings = tableFullName.split("\\.");
        if(strings.length != 3){
            throw new IllegalArgumentException("invalid table full name: " + tableFullName + "', which need like {source}.{schema}.{table}");
        }
        return strings;
    }
}
