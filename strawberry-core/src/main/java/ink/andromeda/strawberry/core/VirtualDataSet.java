//package ink.andromeda.strawberry.core;
//
//import com.alibaba.fastjson.JSONObject;
//import lombok.*;
//import lombok.extern.slf4j.Slf4j;
//import net.wecash.proxima.datasource.entity.Operator;
//import net.wecash.proxima.datasource.entity.QueryParam;
//import net.wecash.proxima.datasource.entity.TableColumn;
//import net.wecash.proxima.datasource.entity.TableMetaInfo;
//import org.apache.commons.lang3.ArrayUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.commons.lang3.tuple.Pair;
//import org.springframework.util.Assert;
//
//import javax.sql.DataSource;
//import java.sql.*;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//import static net.wecash.proxima.datasource.CoreDataService.SIMPLE_TASK_POOL_EXECUTOR;
//import static net.wecash.proxima.datasource.entity.QueryParam.toSqlCondition;
//import static net.wecash.proxima.datasource.tools.GeneralTools.simpleQueryOne;
//import static net.wecash.proxima.datasource.tools.GeneralTools.toJSONString;
//
//
///**
// * 数据集
// */
//@Slf4j
//public class VirtualDataSet {
//
//    /**
//     * 数据集的id
//     */
//    @Getter
//    private final long id;
//
//    /**
//     * 数据集的名称(唯一)
//     */
//    @Getter
//    private final String name;
//
//    /**
//     * 该数据集所包含的表
//     * <p>库名.表名<->表信息</p>
//     */
//    @Getter
//    private final Map<String, TableMetaInfo> originalTables;
//
//    /**
//     * 本数据集所包含的原始表的虚拟关系
//     */
//    private final VirtualRelation virtualRelation;
//
//    /**
//     * 数据源集合
//     */
//    @Setter
//    private Map<Object, DataSource> dataSourceMap;
//
//    /**
//     * <p>首轮查询时单次最大查询数量, 默认为1000。
//     * <p>该值较小时, 单轮连接的计算量小, 但是查询数据库次数较多, 较大时反之, 需要根据limit
//     * 和预估结果合理设置。
//     */
//    @Setter
//    private int baseQueryCount = DEFAULT_BASE_QUERY_COUNT;
//
//    private static final int DEFAULT_BASE_QUERY_COUNT = 1000;
//
//    public VirtualDataSet(long id,
//                          String name,
//                          Map<String, TableMetaInfo> originalTables,
//                          VirtualRelation virtualRelation) {
//        this.id = id;
//        this.name = name;
//        this.originalTables = originalTables;
//        this.virtualRelation = virtualRelation;
//    }
//
//    /**
//     * 获取本数据集的预览数据
//     *
//     * @param limit 查看的条数
//     * @return 结果集
//     * @throws Exception 查询数据集异常
//     */
//    public VirtualResultSet previewDataSet(int limit) throws Exception {
//        return executeQuery(null, limit, true);
//    }
//
//    /**
//     * 执行查询任务, 默认限制1000条查询结果
//     *
//     * @param queryParam 查询参数
//     * @return 结果集
//     * @throws Exception 查询数据集异常
//     */
//    public VirtualResultSet executeQuery(QueryParam queryParam) throws Exception {
//        return executeQuery(queryParam, 1000);
//    }
//
//    /**
//     * 执行查询任务
//     *
//     * @param queryParam 查询参数
//     * @param limit      限制条数
//     * @return 结果集
//     * @throws Exception 查询数据集异常
//     */
//    public VirtualResultSet executeQuery(QueryParam queryParam, int limit) throws Exception {
//        return executeQuery(queryParam, limit, false);
//    }
//
//    /**
//     * 执行查询任务的核心方法
//     *
//     * @param queryParam 查询参数
//     * @param limit      限制条数
//     * @param isPreview  是否为查询预览数据
//     * @return 结果集
//     * @throws Exception 查询过程发生异常
//     */
//    private VirtualResultSet executeQuery(QueryParam queryParam, int limit, boolean isPreview) throws Exception {
//        VirtualResultSet resultSet = new VirtualResultSet();
//        resultSet.setVirtualDataSetName(this.name);
//
//        List<JSONObject> resultData = new ArrayList<>(128);
//
//        // 预览数据集, 没有查询参数
//        if (isPreview) {
//            int start = 0;
//            boolean hasRemain = true;
//
//            List<JSONObject> currentResultData = new ArrayList<>(64);
//
//            String startTableName = virtualRelation.first.leftTable;
//
//            // OriginalResultSet currentResultSet = queryJoinData(startTableName, selectSql);
//
//            String[] nextJoinFields = virtualRelation.getNextJoinFields(startTableName);
//            OriginalResultSet currentResultSet = queryBaseDataWithUnionForPreview(startTableName, nextJoinFields, new String[][]{nextJoinFields}, limit);
//
//            if (currentResultSet.getData().isEmpty()) {
//                log.info("no result");
//            } else {
//
//                int joinCount = virtualRelation.joinConfigChain.size();
//                String nextTable = virtualRelation.getNextTableName(startTableName);
//                currentResultData.addAll(currentResultSet.getData());
//
//                // start += currentResultData.size();
//                // hasRemain = currentResultData.size() == baseQueryCount;
//
//                // 向后连接
//                while (joinCount > 0 && nextTable != null) {
//                    JoinConfig config = virtualRelation.getRightPointIndexMap().get(nextTable);
//
//                    String querySql = genJoinDataQuerySql(config, currentResultSet, new AtomicBoolean(false), null, true);
//
//                    currentResultSet = queryJoinDataWithUnion(nextTable,genJoinConditionSql(
//                            config, currentResultSet, new AtomicBoolean(false), null, true), -1);
//
//                    if (currentResultSet.getData().isEmpty()) {
//                        joinCount--;
//                        while ((nextTable = virtualRelation.getNextTableName(nextTable)) != null) {
//                            joinCount--;
//                        }
//                        break;
//                    }
//
//                    currentResultData = joining(config, currentResultSet, currentResultData, true, false);
//                    nextTable = virtualRelation.getNextTableName(nextTable);
//                    joinCount--;
//                    log.debug("current data size: {}", currentResultData.size());
//                }
//
//                if (joinCount != 0) {
//                    throw new IllegalStateException("illegal join status, join finished but join count not equals config length!");
//                }
//
//                resultData.addAll(currentResultData);
//
//                if (resultData.size() > limit) {
//                    resultData = resultData.subList(0, limit);
//                }
//            }
//            resultSet.setData(resultData);
//            return resultSet;
//        }
//
//        resultSet.setQueryParam(queryParam);
//        List<List<QueryParam.ConditionCase>> paramList = queryParam.getParams();
//
//        for (List<QueryParam.ConditionCase> conditionGroup : paramList) {
//            int start = 0;
//            boolean hasRemain = true;
//            // 分析最优化的查询方式
//            Map<String, List<QueryParam.ConditionCase>> tableConditionGroup = conditionGroup.stream()
//                    .collect(Collectors.groupingBy(c -> c.getFullFieldName().substring(0, c.getFullFieldName().lastIndexOf('.'))));
//            String startMainTableName;
//            try {
//                startMainTableName = analyseQuery(tableConditionGroup);
//            } catch (Exception ex) {
//                // mysql执行计划的分析方式失败
//                log.warn("analyse query exception: {}", ex.toString(), ex);
//                log.info("try another way to found start table");
//                AtomicReference<String> startTable = new AtomicReference<>();
//                // 查找查询条件中索引字段最多的表
//                tableConditionGroup.entrySet().stream().filter(c -> c.getValue().stream().anyMatch(s -> {
//                    String fileName = s.getFullFieldName().substring(s.getFullFieldName().lastIndexOf('.') + 1);
//                    String tableName = s.getFullFieldName().substring(0, s.getFullFieldName().lastIndexOf('.'));
//                    return originalTables.get(tableName).getColumns().get(fileName).isIndex();
//                })).max((c1, c2) -> Long.compare(c1.getValue().stream().filter(s -> {
//                    String fileName = s.getFullFieldName().substring(s.getFullFieldName().lastIndexOf('.') + 1);
//                    String tableName = s.getFullFieldName().substring(0, s.getFullFieldName().lastIndexOf('.'));
//                    return originalTables.get(tableName).getColumns().get(fileName).isIndex();
//                }).count(), c2.getValue().stream().filter(s -> {
//                    String fileName = s.getFullFieldName().substring(s.getFullFieldName().lastIndexOf('.') + 1);
//                    String tableName = s.getFullFieldName().substring(0, s.getFullFieldName().lastIndexOf('.'));
//                    return originalTables.get(tableName).getColumns().get(fileName).isIndex();
//                }).count())).ifPresent(e -> startTable.set(e.getKey()));
//
//                if (startTable.get() == null)
//                    throw new IllegalStateException("where condition has no index field!, conditions: " + toJSONString(tableConditionGroup));
//                startMainTableName = startTable.get();
//            }
//
//            Iterator<UnionConfig> unionConfigIterator = Optional.ofNullable(virtualRelation.getUnionConfigs(startMainTableName))
//                    .orElse(Collections.emptyList())
//                    .iterator();
//            List<QueryParam.ConditionCase> startTableConditions = tableConditionGroup.get(startMainTableName);
//            String startTableName = startMainTableName;
//            String startTableConditionSql = toSqlCondition(startTableConditions);
//
//            while (hasRemain) {
//                List<JSONObject> currentResultData = new ArrayList<>(64);
//
//                String selectSql = String.format("SELECT * FROM %s WHERE %s", startTableName, startTableConditionSql);
//
//                selectSql += String.format(" LIMIT %s,%s", start, baseQueryCount);
//
//                OriginalResultSet currentResultSet = queryJoinData(startMainTableName, selectSql);
//
//                // 首轮查询为空, 标记为无结果并退出当前条件组的查询
//                if (currentResultSet.getData().isEmpty()) {
//                    hasRemain = false;
//                    log.info("no result");
//                    break;
//                }
//
//                int joinCount = virtualRelation.joinConfigChain.size();
//                String prevTable = virtualRelation.getPrevTableName(startMainTableName);
//                String nextTable = virtualRelation.getNextTableName(startMainTableName);
//                currentResultData.addAll(currentResultSet.getData());
//
//                start += currentResultData.size();
//                hasRemain = currentResultData.size() == baseQueryCount;
//                if (currentResultData.size() < baseQueryCount) {
//                    if (unionConfigIterator.hasNext()) {
//                        UnionConfig unionConfig = unionConfigIterator.next();
//                        startTableName = unionConfig.getUnionTable().fullName();
//                        startTableConditionSql = toSqlCondition(startTableConditions, s -> unionConfig.getMainTableFieldToUnionTableFieldRef().get(s));
//                        start = 0;
//                    }
//                }
//
//
//                OriginalResultSet centre = currentResultSet;
//                log.debug("current data size: {}", currentResultData.size());
//                // 向前连接
//                while (joinCount > 0 && prevTable != null) {
//                    JoinConfig config = virtualRelation.getLeftPointIndexMap().get(prevTable);
//                    AtomicBoolean hasQueryParam = new AtomicBoolean(false);
//                    // String querySql = genJoinDataQuerySql(config, currentResultSet, hasQueryParam, tableConditionGroup.get(prevTable), false);
//                    currentResultSet = queryJoinDataWithUnion(prevTable,
//                            genJoinConditionSql(config, currentResultSet, hasQueryParam, tableConditionGroup.get(prevTable), false),
//                            -1);
//                    if (currentResultSet.getData().isEmpty()) {
//                        while ((prevTable = virtualRelation.getPrevTableName(prevTable)) != null) {
//                            joinCount--;
//                        }
//                        if (hasQueryParam.get()) {
//                            currentResultData.clear();
//                        }
//                        joinCount--;
//                        break;
//                    }
//                    currentResultData = joining(config, currentResultSet, currentResultData, false, hasQueryParam.get());
//                    prevTable = virtualRelation.getPrevTableName(prevTable);
//                    joinCount--;
//                    log.debug("current data size: {}", currentResultData.size());
//                }
//
//                // 向后连接
//                currentResultSet = centre;
//                while (joinCount > 0 && nextTable != null) {
//
//                    JoinConfig config = virtualRelation.getRightPointIndexMap().get(nextTable);
//                    AtomicBoolean hasQueryParam = new AtomicBoolean(false);
//
//                    // String querySql = genJoinDataQuerySql(config, currentResultSet, hasQueryParam, tableConditionGroup.get(nextTable), true);
//                    currentResultSet = queryJoinDataWithUnion(nextTable,
//                            genJoinConditionSql(config, currentResultSet, hasQueryParam, tableConditionGroup.get(nextTable), true),
//                            -1);
//
//                    if (currentResultSet.getData().isEmpty()) {
//                        // 如果没有外部查询参数, 则之后的连接数据均为空
//                        while ((nextTable = virtualRelation.getNextTableName(nextTable)) != null) {
//                            joinCount--;
//                        }
//                        // 如果查询到的结果为空, 且有外部查询参数, 则整个结果集返回为空
//                        if (hasQueryParam.get()) {
//                            currentResultData.clear();
//                        }
//                        joinCount--;
//                        break;
//                    }
//
//                    currentResultData = joining(config, currentResultSet, currentResultData, true, hasQueryParam.get());
//                    nextTable = virtualRelation.getNextTableName(nextTable);
//                    joinCount--;
//                    log.debug("current data size: {}", currentResultData.size());
//                }
//
//                if (joinCount != 0) {
//                    throw new IllegalStateException("illegal join status, join finished but join count not equals config length!");
//                }
//                resultData.addAll(currentResultData);
//                if (resultData.size() == limit) {
//                    break;
//                }
//                if (resultData.size() > limit) {
//                    resultData = resultData.subList(0, limit);
//                    break;
//                }
//            }
//        }
//        resultSet.setData(resultData);
//        return resultSet;
//    }
//
//    /**
//     * 通过提供的条件分析出结果数可能为最小的表
//     *
//     * @param tableConditionGroup 每个表所包含的条件
//     * @return 查询结果数最少的表
//     */
//    private String analyseQuery(Map<String, List<QueryParam.ConditionCase>> tableConditionGroup) throws IllegalStateException {
//        CountDownLatch countDownLatch = new CountDownLatch(tableConditionGroup.size());
//        Map<Long, String> indexMap = new ConcurrentHashMap<>(tableConditionGroup.size());
//        tableConditionGroup.forEach((k, v) ->
//                SIMPLE_TASK_POOL_EXECUTOR.submit(
//                        () -> {
//                            String sql = String.format("EXPLAIN SELECT * FROM %s WHERE %s ", k, v.stream()
//                                    .map(o ->
//                                            o.getSimpleFieldName() +
//                                            o.getOperator().code() +
//                                            o.getOperator().getSQLStringValue(o.getCompareValue())
//                                    )
//                                    .collect(Collectors.joining(" AND ")));
//                            log.info(sql);
//                            JSONObject res = simpleQueryOne(dataSourceMap.get(k.substring(0, k.indexOf('.'))), sql);
//                            assert res != null;
//                            Long rows = res.getLong("rows");
//                            indexMap.put(rows != null ? rows : Long.MAX_VALUE, k);
//                            log.info("table {} scan max rows is {}.", k, rows);
//                            countDownLatch.countDown();
//                        }
//                )
//        );
//        try {
//            countDownLatch.await();
//
//            long min = Long.MAX_VALUE;
//            for (long i : indexMap.keySet()) {
//                if (i < min)
//                    min = i;
//            }
//
//            if (min == Long.MAX_VALUE)
//                throw new IllegalStateException("could not found min rows table");
//            String startTable = indexMap.get(min);
//            log.info("analyse query success, start query table is {}", startTable);
//            return startTable;
//        } catch (InterruptedException e) {
//            throw new IllegalStateException(e);
//        }
//    }
//
//    ;
//
//    /**
//     * 生成获取连接数据的SQL
//     *
//     * @param config           连接配置
//     * @param currentResultSet 当前结果集
//     * @param hasQueryParam    是否有查询参数
//     * @param tableCondition   查询条件
//     * @param isNextJoin       是否是向后连接的
//     * @return SQL语句
//     */
//    private String genJoinDataQuerySql(JoinConfig config,
//                                       OriginalResultSet currentResultSet,
//                                       AtomicBoolean hasQueryParam,
//                                       List<QueryParam.ConditionCase> tableCondition,
//                                       boolean isNextJoin) {
//        StringBuilder querySql = new StringBuilder();
//
//        /*
//         * next(right) join:
//         *
//         *                           SQL for query--------------------------
//         *                                                                 |
//         *                                                                 |
//         *                                                                 V
//         * left table <=========[left fields<-->right fields]=========> right table
//         *     |
//         *     |
//         *     V
//         * currentResultSet
//         *
//         * SELECT * FROM {right table} WHERE
//         *               {right join field0} IN {value of the corresponding left fields} AND
//         *               {right join field1} IN {value of the corresponding left fields} AND
//         *               ..... AND
//         *               {conditions}
//         *
//         * perv(left) join:
//         * opposite to right join
//         *
//         */
//
//        querySql.append("SELECT * FROM ").append(isNextJoin ? config.getRightTable() : config.getLeftTable()).append(" WHERE ");
//
//        List<String> cs = new ArrayList<>();
//        for (Pair<TableColumn, TableColumn> p : config.getJoinFields()) {
//            Object[] values = new ArrayList<>(currentResultSet.getIndexMap()
//                    .get(isNextJoin ? p.getLeft().fullFieldName() : p.getRight().fullFieldName()).keySet()).toArray();
//            cs.add((isNextJoin ? p.getRight().getName() : p.getLeft().getName()) + " IN " + Operator.IN.getSQLStringValue(values));
//        }
//
//        querySql.append(cs.stream().collect(Collectors.joining(" AND ", " ", " ")));
//        Optional.ofNullable(tableCondition).ifPresent(c -> {
//            querySql.append(" AND ")
//                    .append(c.stream().map(t -> t.getFullFieldName().split("\\.")[2] + t.getOperator().code() + t.getOperator().getSQLStringValue(t.getCompareValue())).collect(Collectors.joining(" AND ")));
//            hasQueryParam.set(true);
//        });
//
//        // TODO 此种方式查询出的数据会多于实际符合要求的数据, 待优化
//
//        return querySql.toString();
//    }
//
//    private List<QueryParam.ConditionCase> genJoinConditionSql(JoinConfig config,
//                                                               OriginalResultSet currentResultSet,
//                                                               AtomicBoolean hasQueryParam,
//                                                               List<QueryParam.ConditionCase> tableCondition,
//                                                               boolean isNextJoin) {
//        List<QueryParam.ConditionCase> cases = new ArrayList<>();
//        for (Pair<TableColumn, TableColumn> p : config.getJoinFields()) {
//            Object[] values = new ArrayList<>(currentResultSet.getIndexMap()
//                    .get(isNextJoin ? p.getLeft().fullFieldName() : p.getRight().fullFieldName()).keySet()).toArray();
//            cases.add(QueryParam.ConditionCase.builder()
//                    .operator(Operator.IN)
//                    .compareValue(values)
//                    .fullFieldName(isNextJoin ? p.getRight().fullFieldName() : p.getLeft().fullFieldName())
//                    .build());
//        }
//        Optional.ofNullable(tableCondition).ifPresent(c -> {
//            cases.addAll(c);
//            hasQueryParam.set(true);
//        });
//
//        return cases;
//    }
//
//    /**
//     * 查询出待连接的数据
//     *
//     * @param joinTable 要连接的表
//     * @param sql       查询sql
//     */
//    private OriginalResultSet queryJoinData(String joinTable,
//                                            String sql) {
//        String[] nextJoinFields = virtualRelation.getNextJoinFields(joinTable);
//        String[] prevJoinFields = virtualRelation.getPrevJoinFields(joinTable);
//        return net.wecash.proxima.datasource.VirtualDataSet.this.queryDataSource(joinTable, sql, Stream.concat(
//                Stream.of(nextJoinFields),
//                Stream.of(prevJoinFields)
//        ).distinct().toArray(String[]::new), new String[][]{nextJoinFields, prevJoinFields});
//    }
//
//    private OriginalResultSet queryJoinDataWithUnion(String joinTable, List<QueryParam.ConditionCase> conditionCases, int limit) {
//        // 和后一个表的连接字段
//        String[] nextJoinFields = virtualRelation.getNextJoinFields(joinTable);
//        // 和前一个表的连接字段
//        String[] prevJoinFields = virtualRelation.getPrevJoinFields(joinTable);
//        return net.wecash.proxima.datasource.VirtualDataSet.this.queryDataWithUnion(joinTable, conditionCases, Stream.concat(
//                Stream.of(nextJoinFields),
//                Stream.of(prevJoinFields)
//        ).distinct().toArray(String[]::new), new String[][]{nextJoinFields, prevJoinFields}, limit);
//    }
//
//
//    /**
//     * 执行数据连接操作
//     *
//     * @param config                 连接配置
//     * @param waitingJoinedResultSet 待连接的数据集
//     * @param currentResultData      当前数据
//     * @param isNextJoin             是否是向后连接
//     * @param hasQueryParam          是否有查询参数
//     * @return 连接完成的数据
//     */
//    private List<JSONObject> joining(JoinConfig config,
//                                     OriginalResultSet waitingJoinedResultSet,
//                                     List<JSONObject> currentResultData,
//                                     boolean isNextJoin,
//                                     boolean hasQueryParam) {
//
//        /*
//         * next(right) join:
//         *
//         *
//         * current joined data <=========[left fields<-->right fields]=========> right table (waiting join)
//         *
//         *
//         *      def: result
//         *
//         *
//         * ------------->----------------------------> currentResultDat >--------------iterate data------------------------
//         * |                                                                                                              |
//         * |     iteration variable of currentResultData: cd                                                              |
//         * |                                                                                                              |
//         * |                                query waitingJoinedResultSet.joinIndexMap                                     |
//         * |                                                   |                                                          |
//         * |                                                   |                                                          |
//         * |                                                 query                                                        V
//         * |                                                   |                                                          |
//         * |                                                   V                                                          |
//         * ^                    ---------------------------------------------------------------                           |
//         * |                    | indexName: right fields                                     |                           |
//         * |                    | indexKey:  value of the corresponding left fields(from cd)  |                           |
//         * |                    ---------------------------------------------------------------                           |
//         * |                                                   |                                                          V
//         * |                                                   |                                                          |
//         * |                                                   V                                                          |
//         * |                         --------------------can joined data------------------                                |
//         * |                         |                                                   |                                |
//         * |                         V                                                   V                                |
//         * |          size is 0 and has no query param                           size is 1 or more:                       |
//         * ^                         |                                                   |                                V
//         * |                         V                                                   V                                |
//         * |                  -------------------                     ---------------------------------------             |
//         * |                  |  result add cd  |                     |  joined data.forEach: jd ->         |             |
//         * |                  -------------------                     |       result add (cd combine jd)    |             |
//         * |                                                          ---------------------------------------             |
//         * |                                                                                                              |
//         * -----------------------<----------------------<--------------------------<-----------------------<--------------
//         *
//         *
//         * prev(left) join opposite to above
//         *
//         */
//        List<JSONObject> newResultData = new ArrayList<>();
//        String indexName = config.getJoinFields().stream()
//                .map(f -> isNextJoin ? f.getRight().fullFieldName() : f.getLeft().fullFieldName())
//                .collect(Collectors.joining("::"));
//        for (JSONObject cd : currentResultData) {
//            IndexKey indexKey = new IndexKey(config.getJoinFields().stream()
//                    .map(f -> cd.get(isNextJoin ? f.getLeft().fullFieldName() : f.getRight().fullFieldName()))
//                    .toArray());
//            List<JSONObject> joinData = Optional.ofNullable(waitingJoinedResultSet.getJoinIndex().get(indexName).get(indexKey)).orElse(Collections.emptyList());
//            if (joinData.size() == 0 && !hasQueryParam) {
//                newResultData.add(cd);
//            }
//            if (joinData.size() > 0) {
//                for (JSONObject joinDatum : joinData) {
//                    newResultData.add(cd.fluentPutAll(joinDatum));
//                }
//            }
//        }
//
//        log.debug("current data size: {}", currentResultData.size());
//        return newResultData;
//    }
//
//
//    private static List<JSONObject> queryFromJoinIndexMap(Map<String, Map<IndexKey, List<JSONObject>>> compositeIndexMap, List<QueryParam.ConditionCase> cases) {
//
//
//        return null;
//
//    }
//
//    private static List<JSONObject> queryFromIndexingMap(Map<String, Map<Object, List<JSONObject>>> indexMap, List<QueryParam.ConditionCase> cases) {
//        QueryParam.ConditionCase c = cases.stream().min((c1, c2) -> Integer.compare(indexMap.get(c1.getFullFieldName()).get(c1.getCompareValue()[0]).size(),
//                indexMap.get(c2.getFullFieldName()).get(c1.getCompareValue()[0]).size())).orElseThrow(NullPointerException::new);
//        if (indexMap.get(c.getFullFieldName()).get(c.getCompareValue()[0]) == null)
//            return Collections.emptyList();
//        return indexMap.get(c.getFullFieldName()).get(c.getCompareValue()[0]).stream().filter(s -> {
//            for (QueryParam.ConditionCase ca : cases) {
//                if (!Objects.equals(s.get(ca.getFullFieldName()), ca.getCompareValue()[0]))
//                    return false;
//            }
//            return true;
//        }).collect(Collectors.toList());
//    }
//
//
//    public static class VirtualTable {
//
//        private List<JSONObject> current;
//
//        @Setter
//        private Map<String, OriginalResultSet> sourceResultSetList;
//
//        public OriginalResultSet getSourceResultSet(String tableName) {
//            return Objects.requireNonNull(sourceResultSetList.get(tableName));
//        }
//
//        public void addSourceResult(String sourceTableName, List<JSONObject> data) {
//            OriginalResultSet originalResultSet = Objects.requireNonNull(sourceResultSetList.get(sourceTableName), "current virtual table has no source result set '" + sourceTableName + "'");
//            originalResultSet.setData(data);
//        }
//
//    }
//
//
//    /**
//     * 数据集的查询结果
//     */
//    @Data
//    public static class VirtualResultSet {
//
//        private String name;
//
//        private QueryParam queryParam;
//
//        private String sql;
//
//        private String virtualDataSetName;
//
//        private List<JSONObject> data = new ArrayList<>(64);
//
//
//    }
//
//
//    /**
//     * 查询原始数据
//     *
//     * @param fullTableName  表名的全称(库名.表名)
//     * @param sql            SQL语句
//     * @param indexFields    索引字段
//     * @param compositeIndex 组合索引字段
//     * @return 原始表结果集
//     */
//    private OriginalResultSet queryDataSource(String fullTableName, String sql, String[] indexFields, String[][] compositeIndex) {
//        sql = sql.trim();
//        String[] sarr = fullTableName.split("\\.");
//        if (sarr.length != 2)
//            throw new IllegalArgumentException(String.format("your table name %s is invalid, must contains schema name, format [schema.table]", fullTableName));
//        return queryDataSource(sarr[0], sarr[1], sql, indexFields, compositeIndex);
//    }
//
//    /**
//     * 查询原始数据
//     *
//     * @param schemaName     库名
//     * @param tableName      表名
//     * @param sql            sql语句
//     * @param indexFields    需要索引的字段
//     * @param compositeIndex 组合索引字段
//     * @return 原始表结果集
//     */
//    private OriginalResultSet queryDataSource(String schemaName, String tableName, String sql, String[] indexFields, String[][] compositeIndex) {
//        Assert.isTrue(StringUtils.isNotBlank(schemaName), "schemaName must be not null!");
//        Assert.isTrue(StringUtils.isNotBlank(tableName), "tableName must be not null!");
//        Assert.isTrue(StringUtils.isNotBlank(sql), "sql must be not null!");
//
//        DataSource dataSource = Objects.requireNonNull(dataSourceMap.get(schemaName), "could not found data source '" + schemaName + "'!");
//        try (
//                Connection connection = dataSource.getConnection();
//                PreparedStatement statement = connection.prepareStatement(sql);
//                ResultSet resultSet = statement.executeQuery();
//        ) {
//            List<JSONObject> data = new ArrayList<>(32);
//            ResultSetMetaData metaData = resultSet.getMetaData();
//
//            boolean hasIndex = indexFields != null && indexFields.length > 0;
//            boolean hasCompositeIndex = compositeIndex != null && compositeIndex.length > 0;
//
//            // 索引名称去重
//            if (hasCompositeIndex) {
//                compositeIndex = distinctIndexName(compositeIndex);
//            }
//            if (hasIndex) {
//                indexFields = distinctIndexName(indexFields);
//            }
//
//            Map<String, Map<Object, List<JSONObject>>> indexingMap = new HashMap<>(hasIndex ? indexFields.length : 0);
//            Map<String, Map<IndexKey, List<JSONObject>>> compositeIndexMap = new HashMap<>(hasCompositeIndex ? compositeIndex.length : 0);
//            int columnCount = metaData.getColumnCount();
//            while (resultSet.next()) {
//                JSONObject object = new JSONObject();
//                for (int i = 1; i <= columnCount; i++) {
//                    String fName = schemaName + "." + tableName + "." + metaData.getColumnName(i);
//                    Object val = resultSet.getObject(i);
//                    object.put(fName, val);
//                }
//                data.add(object);
//                // 建立索引关系
//                if (hasIndex)
//                    buildIndex(indexFields, indexingMap, object);
//                // 建立组合索引
//                if (hasCompositeIndex)
//                    buildIndex(compositeIndex, compositeIndexMap, object);
//            }
//            OriginalResultSet originalResultSet = new OriginalResultSet(schemaName + "." + tableName);
//            originalResultSet.setData(data);
//            originalResultSet.setJoinIndex(compositeIndexMap);
//            originalResultSet.setIndexMap(indexingMap);
//            log.info("execute sql: " + sql);
//            return originalResultSet;
//        } catch (SQLException exception) {
//            throw new IllegalStateException(String.format("error in execute sql [%s], reason: %s", sql, exception.toString()), exception);
//        }
//    }
//
//    private String[] distinctIndexName(String[] index) {
//        return Stream.of(index).distinct().toArray(String[]::new);
//    }
//
//    private String[][] distinctIndexName(String[][] compositeIndex) {
//        // String[]比较时不会比较内部的元素, 将其连接为字符串去重后再还原
//        return Stream.of(compositeIndex)
//                .map(s -> String.join("::", s))
//                .distinct()
//                .map(s -> s.split("::"))
//                .toArray(String[][]::new);
//    }
//
//    private void buildIndex(String[] index,
//                            Map<String, Map<Object, List<JSONObject>>> indexingMap,
//                            JSONObject object) {
//        for (String iFields : index)
//            indexingMap.computeIfAbsent(iFields, k -> new HashMap<>())
//                    .computeIfAbsent(object.get(iFields), k -> new ArrayList<>(8)).add(object);
//    }
//
//    private void buildIndex(String[][] compositeIndex,
//                            Map<String, Map<IndexKey, List<JSONObject>>> compositeIndexMap,
//                            JSONObject object) {
//        for (String[] index : compositeIndex) {
//            Object[] val = Stream.of(index).map(object::get).toArray();
//            String indexName = String.join("::", index);
//            IndexKey indexKey = new IndexKey(val);
//            compositeIndexMap.computeIfAbsent(indexName, k -> new HashMap<>(16))
//                    .computeIfAbsent(indexKey, k -> new ArrayList<>(32)).add(object);
//        }
//    }
//
//
//    /**
//     * union情况下的数据查询
//     *
//     * @deprecated 无法实现两个参数的limit, 可实现的方式效率低下, 待定
//     */
//    private OriginalResultSet queryDataWithUnion(String mainTableFullName, List<QueryParam.ConditionCase> conditionCases,
//                                                 String[] indexFields, String[][] compositeIndex, int... limit) {
//        /*
//        String initLimitSql = "";
//        int limitLength = 0;
//        if (limit != null) {
//            if (limit.length == 1) {
//                initLimitSql = " LIMIT " + limit[0];
//                limitLength = limit[0];
//            } else if (limit.length == 2) {
//                initLimitSql = " LIMIT " + limit[0] + "," + limit[1];
//                limitLength = limit[1];
//            } else
//                throw new IllegalArgumentException("wrong argument 'limit' length: " + limit.length);
//        }
//
//        String sqlTemplate = "SELECT * FROM %s WHERE %s";
//        List<UnionConfig> unionConfigList = virtualRelation.getUnionConfigs(mainTableFullName);
//        if (unionConfigList == null || unionConfigList.size() == 0) {
//            StringBuilder sql = new StringBuilder(String.format(sqlTemplate, mainTableFullName, sqlTemplate)).append(initLimitSql);
//            return queryDataSource(mainTableFullName, sql.toString(), indexFields, compositeIndex);
//        }
//
//        List<JSONObject> data = new ArrayList<>();
//
//        List<String> unionTables = new ArrayList<>(unionConfigList.size() + 1);
//        unionTables.add(mainTableFullName);
//        unionConfigList.stream().map(u -> u.getMainTable().fullName()).forEach(unionTables::add);
//
//
//        boolean hasIndex = indexFields != null && indexFields.length > 0;
//        boolean hasCompositeIndex = compositeIndex != null && compositeIndex.length > 0;
//
//        // 索引名称去重
//        if (hasCompositeIndex) {
//            // String[]比较时不会比较内部的元素, 将其连接为字符串去重后再还原
//            compositeIndex = Stream.of(compositeIndex)
//                    .map(s -> String.join("::", s))
//                    .distinct()
//                    .map(s -> s.split("::"))
//                    .toArray(String[][]::new);
//        }
//        if (hasIndex) {
//            indexFields = Stream.of(indexFields).distinct().toArray(String[]::new);
//        }
//
//        Map<String, Map<Object, List<JSONObject>>> indexingMap = new HashMap<>(hasIndex ? indexFields.length : 0);
//        Map<String, Map<IndexKey, List<JSONObject>>> compositeIndexMap = new HashMap<>(hasCompositeIndex ? compositeIndex.length : 0);
//
//        int count = 0;
//        String sql = "";
//        try {
//            for (String tableFullName : unionTables) {
//                boolean isMain = Objects.equals(tableFullName, mainTableFullName);
//                UnionConfig unionConfig = virtualRelation.getUnionConfig(mainTableFullName, tableFullName);
//                String whereSql = "";
//                if (!CollectionUtils.isEmpty(conditionCases)) {
//                    if (isMain) {
//                        whereSql = toSqlCondition(conditionCases);
//                    } else {
//                        whereSql = toSqlCondition(conditionCases, s -> unionConfig.getMainTableFieldToUnionTableFieldRef().get(s));
//                    }
//                }
//                sql = String.format(sqlTemplate, tableFullName) + whereSql + initLimitSql;
//                String schema = tableFullName.split("\\.")[0];
//                DataSource dataSource = Objects.requireNonNull(dataSourceMap.get(schema), "could not found data source '" + schema + "'!");
//                try (
//                        Connection connection = dataSource.getConnection();
//                        PreparedStatement statement = connection.prepareStatement(sql);
//                        ResultSet resultSet = statement.executeQuery();
//                ) {
//                    ResultSetMetaData metaData = resultSet.getMetaData();
//                    int columnCount = metaData.getColumnCount();
//                    while (resultSet.next()) {
//                        if (limit != null && count >= limitLength)
//                            break;
//                        JSONObject object = new JSONObject();
//                        for (int i = 1; i <= columnCount; i++) {
//                            String fName = isMain ? tableFullName + "." + metaData.getColumnName(i)
//                                    : (mainTableFullName + "." + unionConfig.getUnionTableFieldToMainTableFieldRef().get(metaData.getColumnName(i)));
//                            Object val = resultSet.getObject(i);
//                            object.put(fName, val);
//                        }
//                        data.add(object);
//                        // 建立索引关系
//                        if (hasIndex) {
//                            for (String iFields : indexFields) {
//                                indexingMap.computeIfAbsent(iFields, k -> new HashMap<>())
//                                        .computeIfAbsent(object.get(iFields), k -> new ArrayList<>(8)).add(object);
//                            }
//                        }
//                        // 建立组合索引
//                        if (hasCompositeIndex) {
//                            for (String[] index : compositeIndex) {
//                                Object[] val = Stream.of(index).map(object::get).toArray();
//                                String indexName = String.join("::", index);
//                                IndexKey indexKey = new IndexKey(val);
//                                compositeIndexMap.computeIfAbsent(indexName, k -> new HashMap<>(16))
//                                        .computeIfAbsent(indexKey, k -> new ArrayList<>(32)).add(object);
//                            }
//                        }
//                        count++;
//                    }
//                }
//            }
//
//            OriginalResultSet originalResultSet = new OriginalResultSet(mainTableFullName);
//            originalResultSet.setJoinIndex(compositeIndexMap);
//            originalResultSet.setIndexMap(indexingMap);
//            originalResultSet.setData(data);
//            return originalResultSet;
//        } catch (SQLException ex) {
//            throw new IllegalStateException(String.format("error in execute sql [%s], reason: %s", sql, ex.toString()), ex);
//
//        }
//        */
//        return null;
//    }
//
//    private OriginalResultSet queryDataWithUnion(String mainTableFullName, List<QueryParam.ConditionCase> conditionCases,
//                                                 String[] indexFields, String[][] compositeIndex, int limit) {
//
//        String limitSql = limit <= 0 ? "" : " LIMIT " + limit;
//
//        String sqlTemplate = "SELECT * FROM %s WHERE %s";
//        List<UnionConfig> unionConfigList = virtualRelation.getUnionConfigs(mainTableFullName);
//        if (unionConfigList == null || unionConfigList.size() == 0) {
//            StringBuilder sql = new StringBuilder(String.format(sqlTemplate, mainTableFullName, toSqlCondition(conditionCases))).append(limitSql);
//            return queryDataSource(mainTableFullName, sql.toString(), indexFields, compositeIndex);
//        }
//
//        List<JSONObject> data = new ArrayList<>();
//
//        boolean hasIndex = ArrayUtils.isNotEmpty(indexFields);
//        boolean hasCompositeIndex = ArrayUtils.isNotEmpty(compositeIndex);
//
//        if (hasCompositeIndex) {
//            compositeIndex = distinctIndexName(compositeIndex);
//        }
//        if (hasIndex) {
//            indexFields = distinctIndexName(indexFields);
//        }
//
//        Map<String, Map<Object, List<JSONObject>>> indexingMap = new HashMap<>(hasIndex ? indexFields.length : 0);
//        Map<String, Map<IndexKey, List<JSONObject>>> compositeIndexMap = new HashMap<>(hasCompositeIndex ? compositeIndex.length : 0);
//
//        int count = 0;
//        String sql = "wait init...";
//        Iterator<UnionConfig> unionConfigIterator = unionConfigList.iterator();
//        try {
//            String currentTableFullName = mainTableFullName;
//            String currentConditionSql = toSqlCondition(conditionCases);
//            Function<String, String> fieldNameMapping = s -> s;
//            for (; ; ) {
//                sql = String.format(sqlTemplate, currentTableFullName) + currentConditionSql + limitSql;
//                String schema = currentTableFullName.split("\\.")[0];
//                DataSource dataSource = Objects.requireNonNull(dataSourceMap.get(schema), "could not found data source '" + schema + "'!");
//                try (
//                        Connection connection = dataSource.getConnection();
//                        PreparedStatement statement = connection.prepareStatement(sql);
//                        ResultSet resultSet = statement.executeQuery();
//                ) {
//                    ResultSetMetaData metaData = resultSet.getMetaData();
//                    int columnCount = metaData.getColumnCount();
//                    while (resultSet.next()) {
//                        if (limit > 0 && count >= limit)
//                            break;
//                        JSONObject object = new JSONObject();
//                        for (int i = 1; i <= columnCount; i++) {
//                            String fName = currentTableFullName + "." + fieldNameMapping.apply(metaData.getColumnName(i));
//                            Object val = resultSet.getObject(i);
//                            object.put(fName, val);
//                        }
//                        data.add(object);
//                        // 建立索引关系
//                        if (hasIndex) {
//                            buildIndex(indexFields, indexingMap, object);
//                        }
//                        // 建立组合索引
//                        if (hasCompositeIndex) {
//                            buildIndex(compositeIndex, compositeIndexMap, object);
//                        }
//                        count++;
//                    }
//                }
//                log.info("execute sql: [{}]", sql);
//                if (count >= limit)
//                    break;
//                if (unionConfigIterator.hasNext()) {
//                    UnionConfig unionConfig = unionConfigIterator.next();
//                    currentTableFullName = unionConfig.getUnionTable().fullName();
//                    currentConditionSql = toSqlCondition(conditionCases, s -> unionConfig.mainTableFieldToUnionTableFieldRef.get(s));
//                    fieldNameMapping = s -> unionConfig.unionTableFieldToMainTableFieldRef.get(s);
//                } else {
//                    break;
//                }
//            }
//            OriginalResultSet originalResultSet = new OriginalResultSet(mainTableFullName);
//            originalResultSet.setJoinIndex(compositeIndexMap);
//            originalResultSet.setIndexMap(indexingMap);
//            originalResultSet.setData(data);
//            return originalResultSet;
//        } catch (SQLException ex) {
//            throw new IllegalStateException(String.format("error in execute sql [%s], reason: %s", sql, ex.toString()), ex);
//        }
//    }
//
//    private OriginalResultSet queryBaseDataWithUnionForPreview(String mainTableFullName, String[] indexFields, String[][] compositeIndex, int limit) {
//
//        final String limitSql = " LIMIT " + limit;
//
//        //String sqlTemplate = "SELECT * FROM %s WHERE %s";
//        List<UnionConfig> unionConfigList = Optional.ofNullable(virtualRelation.getUnionConfigs(mainTableFullName)).orElse(Collections.emptyList());
////        if (unionConfigList == null || unionConfigList.size() == 0) {
////            StringBuilder sql = new StringBuilder(String.format(sqlTemplate, mainTableFullName, "")).append(limitSql);
////            return queryDataSource(mainTableFullName, sql.toString(), indexFields, compositeIndex);
////        }
//
//        List<JSONObject> data = Collections.synchronizedList(new ArrayList<>());
//
//        boolean hasIndex = ArrayUtils.isNotEmpty(indexFields);
//        boolean hasCompositeIndex = ArrayUtils.isNotEmpty(compositeIndex);
//
//        if (hasCompositeIndex) {
//            compositeIndex = distinctIndexName(compositeIndex);
//        }
//        if (hasIndex) {
//            indexFields = distinctIndexName(indexFields);
//        }
//
//        Map<String, Map<Object, List<JSONObject>>> indexingMap = new HashMap<>(hasIndex ? indexFields.length : 0);
//        Map<String, Map<IndexKey, List<JSONObject>>> compositeIndexMap = new HashMap<>(hasCompositeIndex ? compositeIndex.length : 0);
//
//        List<String> tables = new ArrayList<>();
//        tables.add(mainTableFullName);
//        unionConfigList.stream().map(u -> u.getUnionTable().fullName()).forEach(tables::add);
//        AtomicInteger count = new AtomicInteger();
//
//        CountDownLatch countDownLatch = new CountDownLatch(tables.size());
//        for (String tableFullName : tables) {
//            boolean isMain = Objects.equals(tableFullName, mainTableFullName);
//            UnionConfig unionConfig = isMain ? null : virtualRelation.getUnionConfig(mainTableFullName, tableFullName);
//            final String[] finalIndexFields = indexFields;
//            final String[][] finalCompositeIndex = compositeIndex;
//            SIMPLE_TASK_POOL_EXECUTOR.execute(() -> {
//                String sql = String.format("SELECT * FROM %s ", tableFullName) + limitSql;
//                String schema = tableFullName.split("\\.")[0];
//                DataSource dataSource = Objects.requireNonNull(dataSourceMap.get(schema), "could not found data source '" + schema + "'!");
//                try (
//                        Connection connection = dataSource.getConnection();
//                        PreparedStatement statement = connection.prepareStatement(sql);
//                        ResultSet resultSet = statement.executeQuery();
//                ) {
//                    ResultSetMetaData metaData = resultSet.getMetaData();
//                    int columnCount = metaData.getColumnCount();
//                    while (resultSet.next()) {
//                        if (count.get() >= limit)
//                            break;
//                        JSONObject object = new JSONObject();
//                        for (int i = 1; i <= columnCount; i++) {
//                            String fName = tableFullName + "." +
//                                           (isMain ? metaData.getColumnName(i) :
//                                                   unionConfig.unionTableFieldToMainTableFieldRef.get(metaData.getColumnName(i)));
//                            Object val = resultSet.getObject(i);
//                            object.put(fName, val);
//                        }
//                        data.add(object);
//                        // 建立索引关系
//                        if (hasIndex) {
//                            buildIndex(finalIndexFields, indexingMap, object);
//                        }
//                        // 建立组合索引
//                        if (hasCompositeIndex) {
//                            buildIndex(finalCompositeIndex, compositeIndexMap, object);
//                        }
//                        count.incrementAndGet();
//                    }
//                    log.info("execute sql: [{}]", sql);
//                } catch (SQLException ex) {
//                    throw new IllegalStateException(String.format("error in execute sql [%s], reason: %s", sql, ex.toString()), ex);
//                } finally {
//                    countDownLatch.countDown();
//                }
//            });
//        }
//
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException e) {
//            log.error(e.toString(), e);
//            e.printStackTrace();
//        }
//        List<JSONObject> finalData = new ArrayList<>(data);
//        if (finalData.size() > limit)
//            finalData = finalData.subList(0, limit);
//        OriginalResultSet originalResultSet = new OriginalResultSet(mainTableFullName);
//        originalResultSet.setJoinIndex(compositeIndexMap);
//        originalResultSet.setIndexMap(indexingMap);
//        originalResultSet.setData(finalData);
//        return originalResultSet;
//
//    }
//
//
//    /**
//     * 原始表的查询结果
//     */
//    public static class OriginalResultSet {
//
//        @Getter
//        private final String name;
//
//        @Getter
//        private boolean hasResult = false;
//
//        @Setter
//        @Getter
//        private String[] indexFields;
//
//        @Getter
//        private List<JSONObject> data = new ArrayList<>();
//
//        // 结果集的索引
//        @Getter
//        @Setter
//        private Map<String, Map<Object, List<JSONObject>>> indexMap;
//
//        // 连接字段的组合索引
//        @Setter
//        @Getter
//        private Map<String, Map<IndexKey, List<JSONObject>>> joinIndex;
//
//        public OriginalResultSet(String name) {
//            this.name = name;
//        }
//
//        public void setData(List<JSONObject> data) {
//            if (hasResult)
//                throw new IllegalStateException("this source result set '" + name + "' has set data!");
//            this.data = data;
//            hasResult = true;
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            return this == o ||
//                   (o instanceof OriginalResultSet && Objects.equals(name, ((OriginalResultSet) o).name));
//        }
//
//        @Override
//        public int hashCode() {
//            return Objects.hash(name);
//        }
//    }
//
//    @Data
//    public static class UnionConfig {
//
//        private TableMetaInfo mainTable;
//
//        private TableMetaInfo unionTable;
//
//        private Map<String, String> unionTableFieldToMainTableFieldRef;
//
//        private Map<String, String> mainTableFieldToUnionTableFieldRef;
//    }
//
//    /**
//     * 以具体值为索引的key
//     */
//    private static class IndexKey {
//
//        private final Object[] values;
//
//        private IndexKey(Object[] values) {
//            Objects.requireNonNull(values);
//            this.values = values;
//        }
//
//        public static IndexKey generateIndexingKey(Object... values) {
//            return new IndexKey(values);
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            return this == o || (o instanceof IndexKey && Arrays.equals(values, ((IndexKey) o).values));
//        }
//
//        @Override
//        public int hashCode() {
//            return Arrays.hashCode(values);
//        }
//
//    }
//
//
//    @Override
//    public String toString() {
//        return "VirtualDataSet{" +
//               "id=" + id +
//               ", name='" + name + '\'' +
//               ", virtualRelation=" + virtualRelation +
//               '}';
//    }
//}
