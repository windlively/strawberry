package ink.andromeda.strawberry.core;


import ink.andromeda.strawberry.entity.IndexKey;
import ink.andromeda.strawberry.entity.JoinType;
import ink.andromeda.strawberry.entity.TableMetaInfo;
import ink.andromeda.strawberry.tools.Pair;
import ink.andromeda.strawberry.tools.SQLTemplate;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ink.andromeda.strawberry.context.StrawberryService.SIMPLE_TASK_POOL_EXECUTOR;
import static ink.andromeda.strawberry.tools.GeneralTools.*;

/**
 * 跨源查询引擎
 */
@Slf4j
public class CrossSourceQueryEngine {

    /**
     * <p>首轮查询时单次最大查询数量, 默认为1000。
     * <p>该值较小时, 单轮连接的计算量小, 但是查询数据库次数较多, 较大时反之, 需要根据limit
     * 和预估结果合理设置。
     */
    @Setter
    private int baseQueryCount = DEFAULT_BASE_QUERY_COUNT;

    /**
     * 结果集的最大长度, 超出该数量后剩余数据将被忽略
     */
    @Setter
    private int maxResultLength = DEFAULT_MAX_RESULT_LENGTH;

    private static final int DEFAULT_BASE_QUERY_COUNT = 1000;

    /**
     * 最大查询长度
     */
    private static final int DEFAULT_MAX_RESULT_LENGTH = 20000;

    public final static String $_LINE_NUMBER_STR = "$LINE_NUMBER";


    // 执行join操作时的标识符

    /**
     * 只保留能join到的数据
     */
    private final static byte INNER_JOIN = 0;
    /**
     * 保留两边全部数据
     */
    private final static byte FULL_JOIN = 1;
    /**
     * 以已连接数据为主
     */
    private final static byte MASTER_AS_JOINED_DATA = 2;
    /**
     * 以待连接的数据为准
     */
    private final static byte MASTER_AS_WAITING_JOIN_DATA = 3;

    public CrossSourceQueryEngine(Function<String, TableMetaInfo> obtainTableMetaInfoFunction,
                                  Function<String, DataSource> obtainDataSourceFunction) {

        this.obtainTableMetaInfoFunction = Objects.requireNonNull(obtainTableMetaInfoFunction);
        this.obtainDataSourceFunction = Objects.requireNonNull(obtainDataSourceFunction);
    }

    /**
     * 根据表全名获取数据库表元信息的function
     * 表名格式: {source}.{schema}.{table}
     */
    private final Function<String, TableMetaInfo> obtainTableMetaInfoFunction;

    /**
     * 根据数据源名称获取数据源的function
     */
    private final Function<String, DataSource> obtainDataSourceFunction;

    public QueryResults executeQuery(String sql) throws Exception {

        CrossSourceSQLParser crossSourceSQLParser = new CrossSourceSQLParser(sql);

        log.debug("start query: {}", crossSourceSQLParser.getSql());
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("parser sql");
        LinkRelation linkRelation = crossSourceSQLParser.analysisRelation();
        QueryCondition queryCondition = crossSourceSQLParser.analysisWhereCondition();
        stopWatch.stop();
        stopWatch.start("execute");
        QueryResults results = executeQuery(linkRelation, queryCondition);
        stopWatch.stop();
        log.debug(stopWatch.prettyPrint());
        return results;
    }

    public QueryResults executeQuery(LinkRelation linkRelation, QueryCondition queryCondition) throws Exception {

        String drivingTable = analysisDrivingTable(linkRelation, queryCondition);

        Map<String, LinkRelation.TableNode> virtualNodeMap = linkRelation.getVirtualNodeMap();

        Set<String> remainTables = new HashSet<>(virtualNodeMap.keySet());

        Map<String, String> tableLabelRef = linkRelation.getTableLabelRef();

        // 多线程查询原始表元信息
        CountDownLatch countDownLatch = new CountDownLatch(tableLabelRef.size());
        List<List<String>> fs = new ArrayList<>();
        linkRelation.getTables().forEach((labelName) -> {
            List<String> list = new ArrayList<>();
            String fullName = Objects.requireNonNull(tableLabelRef.get(labelName));
            fs.add(list);
            SIMPLE_TASK_POOL_EXECUTOR.submit(() -> {
                try {
                    TableMetaInfo tableMetaInfo =
                            Objects.requireNonNull(obtainTableMetaInfoFunction.apply(fullName),
                                    String.format("could not found table '%s(%s)' meta info", labelName, fullName));
                    list.addAll(tableMetaInfo.fieldList()
                            .stream()
                            .map(s -> labelName + "." + s)
                            .collect(Collectors.toList()));
                } catch (Exception ex) {
                    log.error(ex.toString(), ex);
                } finally {
                    countDownLatch.countDown();
                }
            });
        });

        List<Map<String, Object>> result = executeQuery(drivingTable, linkRelation, queryCondition, remainTables, queryCondition.limit());


        countDownLatch.await();
        // 输出字段映射关系
        if(linkRelation.getOutputFields() == null){
            if(!CollectionUtils.isEmpty(linkRelation.getOutputDescription())) {
                List<String> outputFields = new ArrayList<>();
                List<String> outputFieldLabels = new ArrayList<>();
                linkRelation.getOutputDescription().forEach(desc -> {
                    String fName = desc.getLeft();

                    if (fName.endsWith(".*")) {
                        String tableLabelName = fName.substring(0, fName.indexOf('.'));
                        String fullName = Objects.requireNonNull(tableLabelRef.get(tableLabelName));
                        List<String> list = obtainTableMetaInfoFunction.apply(fullName).fieldList();
                        outputFields.addAll(list);
                        outputFieldLabels.addAll(list);
                        return;
                    }

                    outputFields.add(desc.getLeft());
                    outputFieldLabels.add(desc.getRight());
                });
                linkRelation.setOutputFields(outputFields);
                linkRelation.setOutputFieldLabels(outputFieldLabels);
            }else {
                List<String> fList = fs.stream().flatMap(Collection::stream).collect(Collectors.toList());
                linkRelation.setOutputFields(fList);
                linkRelation.setOutputFieldLabels(fList);
            }
        }
        QueryResults resultSet = new QueryResults(linkRelation.getSql() + " " + queryCondition.sqlWhereClause(), result, linkRelation.getOutputFields().toArray(new String[0]), linkRelation.getOutputFieldLabels().toArray(new String[0]));
        log.debug("data size: {}", result.size());
        return resultSet;
    }

    /**
     * 递归查询
     *
     * @param currentTableLabelName 当前表名
     * @param asRight               是否在右侧
     * @param joinProfile           连接配置
     * @param currentData           当前已有数据
     * @param refData               参考数据(上一轮查询结果, 与当前数据有连接关系)
     * @param relation              链接关系
     * @param remainTables          剩余表
     */
    private void recursiveQuery(String currentTableLabelName,
                                boolean asRight,
                                JoinProfile joinProfile,
                                AtomicReference<List<Map<String, Object>>> currentData,
                                IntermediateResultSet refData,
                                LinkRelation relation,
                                QueryCondition condition,
                                Set<String> remainTables) {
        // 如果剩余的表不包含当前表, 则表示当前表已经处理过, 退出查询,
        if (!remainTables.contains(currentTableLabelName))
            return;
        // 当前表节点
        LinkRelation.TableNode currentNode = relation.getVirtualNodeMap().get(currentTableLabelName);
        Set<Pair<String, String>> joinFieldPairs = joinProfile.joinFields();
        JoinType joinType = joinProfile.joinType();
        byte joinIdentifier = getJoinIdentifier(asRight, joinType);
        boolean hasQueryParam = condition.conditions().get(currentTableLabelName) != null;

        // 已有结果数据为空
        if (currentData.get().isEmpty()) {

        }
        IntermediateResultSet intermediateResultSet = refData;
        if (!refData.data.isEmpty()) {
            String tableFullName = relation.getTableLabelRef().get(currentTableLabelName);
            String[] splitTableFullName = splitTableFullName(tableFullName);
            String source = splitTableFullName[0];
            String schema = splitTableFullName[1];
            String tableName = splitTableFullName[2];

            DataSource dataSource = getNonNullDataSource(source);
            StringBuilder SQL = new StringBuilder("SELECT * FROM " + schema + "." + tableName + " WHERE ");

            List<String> joinFields = asRight ? joinFieldPairs.stream().map(Pair::getRight).collect(Collectors.toList()) :
                    joinFieldPairs.stream().map(Pair::getLeft).collect(Collectors.toList());

            String[] refJoinFields = asRight ? joinFieldPairs.stream().map(Pair::getLeft).toArray(String[]::new) :
                    joinFieldPairs.stream().map(Pair::getRight).toArray(String[]::new);

            StringBuilder[] joinStatements = new StringBuilder[joinFieldPairs.size()];
            int i = 0;
            for (Pair<String, String> pair : joinFieldPairs) {
                StringBuilder statement = new StringBuilder();
                String joinField = subStringAt(asRight ? pair.getRight() : pair.getLeft(), '.');
                String refJoinField = asRight ? pair.getLeft() : pair.getRight();
                statement.append(joinField).append(" IN ");
                statement.append(refData.index.get(refJoinField).keySet().stream()
                        .map(s -> javaObjectToSQLStringValue(s.values()[0])).collect(Collectors.joining(",", "(", ")")));
                joinStatements[i++] = statement;
            }

            SQL.append(String.join(" AND ", joinStatements));
            if (hasQueryParam) {
                SQL.append(" AND ");
                SQL.append(toSQLCondition(currentTableLabelName, tableName, condition.conditions().get(currentTableLabelName)));
            }

            LinkRelation.TableNode node = relation.getVirtualNodeMap().get(currentTableLabelName);

            String[][] index = findIndex(node);
            intermediateResultSet = new IntermediateResultSet(currentTableLabelName, index);
            try (
                    Connection connection = dataSource.getConnection();
                    PreparedStatement statement = connection.prepareStatement(SQL.toString());
                    ResultSet resultSet = statement.executeQuery();
            ) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                String[] labelNames = new String[columnCount + 1];
                for (i = 1; i <= columnCount; i++) {
                    labelNames[i] = currentTableLabelName + "." + metaData.getColumnName(i);
                }
                int lineNumber = 0;
                /*
                 * 也可以查询出的数据为基准, 再去join前面的数据
                 *
                 */
                while (resultSet.next()) {
                    Map<String, Object> object = new HashMap<>(columnCount);
                    for (i = 1; i <= columnCount; i++) {
                        object.put(labelNames[i], resultSet.getObject(i));
                    }
                    object.put($_LINE_NUMBER_STR, lineNumber++);
                    intermediateResultSet.add(object);
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }

            List<Map<String, Object>> newResult = new ArrayList<>(currentData.get().size());
            if (intermediateResultSet.data.isEmpty()) {
                if (hasQueryParam
                    || joinIdentifier == INNER_JOIN
                    || joinIdentifier == MASTER_AS_WAITING_JOIN_DATA) {
                    currentData.get().clear();
                }
            } else {
                String indexName = String.join(":",
                        joinFieldPairs.stream().map(p -> asRight ? p.getRight() : p.getLeft()).toArray(String[]::new));
                /*
                 * 执行join操作
                 *
                 */
                Map<IndexKey, List<Map<String, Object>>> indexMap = Objects.requireNonNull(intermediateResultSet.index.get(indexName));
                boolean[] processedData = new boolean[intermediateResultSet.data.size()];
                for (Map<String, Object> current : currentData.get()) {
                    Map<String, Object> newData = new HashMap<>(current);
                    Object[] val = Stream.of(refJoinFields).map(newData::get).toArray();
                    IndexKey indexKey = IndexKey.of(val);
                    List<Map<String, Object>> joinData = Optional.ofNullable(indexMap.get(indexKey)).orElse(Collections.emptyList());
                    if (joinData.size() == 0 && !hasQueryParam) {
                        if (joinIdentifier == FULL_JOIN || joinIdentifier == MASTER_AS_JOINED_DATA)
                            newResult.add(newData);
                    }
                    if (joinData.size() >= 1) {
                        for (Map<String, Object> joinDataItem : joinData) {
                            processedData[(int) joinDataItem.get($_LINE_NUMBER_STR)] = true;
                            newData.putAll(joinDataItem);
                            // 需要重新创建新的副本对象
                            newResult.add(new HashMap<>(newData));
                        }
                    }
                }
                /*
                    if (joinIdentifier == MASTER_AS_WAITING_JOIN_DATA || joinIdentifier == FULL_JOIN) {
                        for (int j = 0; j < processedData.length; j++) {
                            if (!processedData[j])
                                //noinspection unchecked
                                newResult.add((Map<String, Object>) intermediateResultSet.data.get(j)[0]);
                        }
                    }
                 */
                currentData.set(newResult);
            }
            log.info(SQL.toString());
        } else {
            if (!currentData.get().isEmpty()) {
                if (hasQueryParam
                    || joinIdentifier == INNER_JOIN
                    || joinIdentifier == MASTER_AS_WAITING_JOIN_DATA) {
                    currentData.get().clear();
                }
            }
        }
        remainTables.remove(currentTableLabelName);
        nextQuery(currentNode, currentData, relation, condition, intermediateResultSet, remainTables);
    }

    /**
     * 初始轮的查询
     *
     * @param startTableLabelName 初始查询的表名
     * @param relation            链接关系
     * @param remainTables        剩余表
     * @param limit               限制条数, -1为不限制
     * @return 结果数据
     */
    private List<Map<String, Object>> executeQuery(String startTableLabelName, LinkRelation relation, QueryCondition condition,
                                                   Set<String> remainTables, int limit) {
        String tableFullName = relation.getTableLabelRef().get(startTableLabelName);
        String[] splitTableFullName = splitTableFullName(tableFullName);
        String source = splitTableFullName[0];
        String schema = splitTableFullName[1];
        String tableName = splitTableFullName[2];

        DataSource dataSource = getNonNullDataSource(source);
        StringBuilder SQL = new StringBuilder("SELECT * FROM ").append(schema).append(".").append(tableName).append(" WHERE ");
        List<String> wheres = Objects.requireNonNull(condition.conditions().get(startTableLabelName));
        String conditions = toSQLCondition(startTableLabelName, tableName, wheres);
        SQL.append(conditions);
        LinkRelation.TableNode node = relation.getVirtualNodeMap().get(startTableLabelName);


        String[][] index = findIndex(node);

        int start = 0;
        List<Map<String, Object>> result = new ArrayList<>(64);
        while (true) {
            IntermediateResultSet intermediateResultSet = new IntermediateResultSet(startTableLabelName, index);
            String currentSQL = SQL.toString() + " LIMIT " + start + "," + baseQueryCount;
            start += baseQueryCount;
            log.info("driving data query: {}", currentSQL);
            int dataCount = 0;
            try (
                    Connection connection = dataSource.getConnection();
                    PreparedStatement statement = connection.prepareStatement(currentSQL);
                    ResultSet resultSet = statement.executeQuery();
            ) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                String[] labelNames = new String[columnCount + 1];
                for (int i = 1; i <= columnCount; i++) {
                    labelNames[i] = startTableLabelName + "." + metaData.getColumnName(i);
                }
                while (resultSet.next()) {
                    Map<String, Object> object = new HashMap<>(columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        object.put(labelNames[i], resultSet.getObject(i));
                    }
                    intermediateResultSet.add(object);
                    dataCount++;
                }
            } catch (Exception ex) {
                throw new IllegalStateException("exception in execute sql: " + SQL, ex);
            }
            AtomicReference<List<Map<String, Object>>> currentData = new AtomicReference<>(intermediateResultSet.data);
            LinkRelation.TableNode currentNode = relation.getVirtualNodeMap().get(startTableLabelName);

            remainTables.remove(startTableLabelName);
            nextQuery(currentNode, currentData, relation, condition, intermediateResultSet, new HashSet<>(remainTables));
            result.addAll(currentData.get());

            if (maxResultLength > 0 && result.size() > maxResultLength) {
                log.warn("the result size is out of limit size {}, ignore the following data!", maxResultLength);
                finishQuery(result, condition);
                break;
            }

            if (limit > 0 && result.size() >= limit || dataCount < baseQueryCount) {
                finishQuery(result, condition);
                if (limit > 0 && result.size() > limit)
                    result = result.subList(0, limit);
                break;
            }
        }
        return result;
    }

    private void finishQuery(List<Map<String, Object>> result, QueryCondition queryCondition){
        List<ConditionItem> conditionItems = queryCondition.crossSourceCondition();
        // 内部条件筛选
        if (conditionItems != null && conditionItems.size() > 0) {
            result = result.stream().filter(m -> conditionItems.stream().allMatch(c -> c.operator().isTrue(m.get(c.leftField()), new Object[]{m.get(c.rightFields()[0])})))
                    .collect(Collectors.toList());
        }
        List<Pair<String, Boolean>> orderedField = queryCondition.orderedFields();
        // 排序字段
        if (orderedField != null && orderedField.size() > 0) {
            result.sort((o1, o2) -> {
                for (Pair<String, Boolean> of : orderedField) {
                    int i = Operator.objectComparator.compare(o1.get(of.getKey()), o2.get(of.getKey()));
                    if (i != 0) return of.getRight() ? i : -i;
                }
                return 0;
            });
        }
    }

    /**
     * 执行下一轮查询
     *
     * @param currentNode  当前节点(表名)
     * @param currentData  已查询出的数据
     * @param relation     SQL语句的内联关系
     * @param refData      与当前节点Join的数据
     * @param remainTables 剩余未查询的表
     */
    private void nextQuery(LinkRelation.TableNode currentNode,
                           AtomicReference<List<Map<String, Object>>> currentData,
                           LinkRelation relation,
                           QueryCondition condition,
                           IntermediateResultSet refData,
                           Set<String> remainTables) {
        Map<String, JoinProfile> nextList = currentNode.next();
        Map<String, JoinProfile> prevList = currentNode.prev();
        if (!nextList.isEmpty()) {
            nextList.forEach((tableLabel, joinFields) -> recursiveQuery(tableLabel, true, joinFields, currentData, refData, relation, condition, remainTables));
        }

        if (!prevList.isEmpty()) {
            prevList.forEach((tableLabel, joinFields) -> recursiveQuery(tableLabel, false, joinFields, currentData, refData, relation, condition, remainTables));
        }
    }

    private static String[][] findIndex(LinkRelation.TableNode node) {
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
        return Stream.concat(node.prev().values().stream().map(s -> s.joinFields().stream().map(Pair::getRight)),
                node.next().values().stream().map(s -> s.joinFields().stream().map(Pair::getLeft)))
                .map(s -> s.toArray(String[]::new))
                .toArray(String[][]::new);
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
     * 中间查询结果
     */
    private static class IntermediateResultSet {

        @Getter
        private final String name;

        private final String[][] indexField;

        private final Map<String, Map<IndexKey, List<Map<String, Object>>>> index;

        private final List<Map<String, Object>> data = new ArrayList<>(32);

        public List<Map<String, Object>> getMatchData(String indexName, IndexKey indexKey) {
            return Optional.ofNullable(
                    Objects.requireNonNull(this.index.get(indexName),
                            "index name '" + indexName + "' is not exist").get(indexKey))
                    .orElse(Collections.emptyList());
        }

        public void add(Map<String, Object> object) {
            this.data.add(object);
            buildIndex(indexField, index, object);
        }

        private IntermediateResultSet(String name, String[][] indexField) {
            this.name = name;
            List<String[]> indexs = new ArrayList<>(8);
            for (String[] strings : indexField) {
                indexs.add(strings);
                // 添加单字段索引
                if (strings.length > 1) {
                    for (String s : strings) {
                        indexs.add(new String[]{s});
                    }
                }
            }
            this.indexField = distinctIndexName(indexs.toArray(new String[0][0]));
            index = new HashMap<>(indexField.length);
        }
    }

    private static String[][] distinctIndexName(String[][] compositeIndex) {
        // String[]比较时不会比较内部的元素, 将其连接为字符串去重后再还原
        return Stream.of(compositeIndex)
                .map(s -> String.join(":", s))
                .distinct()
                .map(s -> s.split(":"))
                .toArray(String[][]::new);
    }

    @SuppressWarnings("unchecked")
    private static void buildIndex(String[][] compositeIndex,
                                   Map<String, Map<IndexKey, List<Map<String, Object>>>> compositeIndexMap,
                                   Map<String, Object> object) {
        for (String[] index : compositeIndex) {
            Object[] val = Stream.of(index).map(object::get).toArray();
            String indexName = String.join(":", index);
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
    private String analysisDrivingTable(LinkRelation relation, QueryCondition condition) {
        Map<String, List<String>> conditionMap = condition.conditions();
        // 如果只有一个表有查询条件, 直接返回该表名
        if (conditionMap.size() == 1)
            return conditionMap.keySet().stream().findFirst().orElse(null);

        CountDownLatch countDownLatch = new CountDownLatch(conditionMap.size());

        // 会有多线程竞争的bug, TreeSet非线程安全
        // TreeSet<Pair<String, Long>> record = new TreeSet<>(Comparator.comparingLong(Pair::getRight));

        Map<Long, String> record = new ConcurrentHashMap<>(conditionMap.size());

        // 遍历所有表的条件组, 多线程方式
        conditionMap.forEach((k, v) -> {
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
        for (long l : record.keySet()) {
            if (l < minRows) {
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

    /**
     * 获取数据源
     *
     * @param sourceName 数据源名称
     * @return {@link DataSource}
     * @throws NullPointerException 未找到对应数据源
     */
    private DataSource getNonNullDataSource(String sourceName) {
        return Objects.requireNonNull(obtainDataSourceFunction.apply(sourceName), "data source '" + sourceName + "' is null!");
    }

    private static String toSQLCondition(String tableLabelName, String correctTableName, List<String> whereCases) {
        return whereCases.stream().map(c -> c.replaceAll("^\\s*\\b" + tableLabelName + "\\b", correctTableName))
                .collect(Collectors.joining(" AND ", " ", " "));
    }

    private static String[] splitTableFullName(String tableFullName) {
        String[] strings = tableFullName.split("\\.");
        if (strings.length != 3) {
            throw new IllegalArgumentException("invalid table full name: " + tableFullName + "', which need like {source}.{schema}.{table}");
        }
        return strings;
    }

    /**
     * 判断连接时的标识符
     *
     * @param asRight  是否作为右侧表
     * @param joinType 连接类型
     */
    private static byte getJoinIdentifier(boolean asRight, JoinType joinType) {
        if (joinType == JoinType.JOIN)
            return INNER_JOIN;
        if (joinType == JoinType.FULL_JOIN)
            return FULL_JOIN;
        return asRight && joinType == JoinType.RIGHT_JOIN || !asRight && joinType == JoinType.LEFT_JOIN
                ? MASTER_AS_WAITING_JOIN_DATA : MASTER_AS_JOINED_DATA;
    }
}
