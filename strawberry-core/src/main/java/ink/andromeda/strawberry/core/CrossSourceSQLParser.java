package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.entity.JoinType;
import ink.andromeda.strawberry.tools.Pair;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 跨数据源sql解析工具
 */
@Slf4j
public class CrossSourceSQLParser {

    /**
     * 校验SQL格式的正则, 所给的SQL语句必须为如下格式:
     * <p>SELECT * FROM s1.d1.t1 AS t1
     * JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4
     * JOIN s2.d2.t3 AS t3 ON t2.f2 = t3.f3 AND t2.f1 = t3.f1
     * WHERE t1.f2 = 'xxx' AND t1.f2 > 'xxx' AND t3.f1 IN ('xxx', 'xxx') AND t1.f3 BETWEEN 'xxx' AND 'xxx';
     * (PS, sn: 数据源名称, dn: 数据库名称, tn: 表名, fn: 字段名)
     */
    private final static Pattern SQL_FORMAT_REG =
            Pattern.compile("(?i)\\s*(SELECT)\\s+((\\*\\s+)|(\\w+\\.((\\w+(\\s+(AS\\s+)?(\\S+|('.*?')|(\".*?\")))?)|(\\*))((\\s*,)?)\\s*)+)(FROM)\\s+" +
                            "(\\w+(\\.\\w+){2})\\s+(AS\\s+)?\\w+\\s+" +
                            "(\\s*(((LEFT|RIGHT|OUTER|FULL)\\s+)?(JOIN)\\s+(\\w+(\\.\\w+){2}))\\s+(AS\\s+)?\\w+\\s+" +
                            "(ON)\\s+((\\w+\\.\\w+)\\s*=\\s*(\\w+\\.\\w+))(\\s+(AND)\\s+((\\w+\\.\\w+)\\s*=\\s*(\\w+\\.\\w+)))*)+" +
                            "(\\s+(WHERE).*?" +
                            "(ORDER\\s+BY(\\s*\\w+\\.\\w+\\s*(\\bDESC|ASC\\b)?(,\\s*)?)+)?" +
                            "(LIMIT\\s+\\d+\\s+)?)?");

    /**
     * 获取表名的正则
     */
    private final static Pattern FIND_TABLE_REG =
            Pattern.compile("(((?<=((?i)FROM))\\s+(\\w+(\\.\\w+){2})\\s+((?i)AS\\s+)?\\w+\\s+(?=((?i)((LEFT|RIGHT|OUTER|FULL)\\s+)?JOIN)))|((?<=(?i)JOIN)\\s+(\\w+(\\.\\w+){2})\\s+((?i)AS\\s+)?\\w+\\s+(?=((?i)ON))))");

    /**
     * 获取连接条件的表达式
     */
    private final static Pattern FIND_JOIN_FIELD_REG =
            Pattern.compile("(?<=\\s(ON))(\\s|\\S)+?(?=(\\s((JOIN)|(WHERE))\\s))", Pattern.CASE_INSENSITIVE);

    private final static Pattern SPLIT_JOIN_CONDITION_REG =
            Pattern.compile("(\\w+(\\.\\w+){3})\\s*=\\s*(\\w+(\\.\\w+){3})", Pattern.CASE_INSENSITIVE);

    /**
     * SQL中的第一个表
     * <p>ex: "SELECT * FROM s1.d1.t1 AS t1 JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4 ..." --> "s1.d1.t1 AS t1"
     */
    private final static Pattern FIND_FIRST_TABLE_REG =
            Pattern.compile("(?<=(\\bFROM\\b))\\s+(\\w+(\\.\\w+){2})\\s+(AS\\s+)?\\w+\\s+((\\bLEFT|RIGHT|OUTER|FULL\\b)\\s+)?(?=\\bJOIN\\b)", Pattern.CASE_INSENSITIVE);

    /**
     * 分割join语句,
     * <p>ex: "JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4" --> ["s1.d2.t2 t2", "t1.f1 = t2.f1", "t1.f3 = t2.f4"]
     */
    private final static Pattern SPLIT_JOIN_CLAUSE_REG =
            Pattern.compile("((?<=\\bJOIN\\b).+(?=\\bON\\b))|(\\w+\\.\\w+\\s*=\\s*\\w+\\.\\w+)", Pattern.CASE_INSENSITIVE);

    /**
     * 从SQL中找出join子句
     * <p>ex: "JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4"
     */
    private final static Pattern FIND_JOIN_CLAUSE_REG =
            Pattern.compile("((((\\b(LEFT)|(RIGHT)|(OUTER)|(FULL)\\b)\\s+?)?(\\bJOIN\\b)\\s+(\\w+(\\.\\w+){2}))\\s+((?i)AS\\s+)?\\w+\\s+" +
                            "((?i)ON)\\s+((\\w+\\.\\w+)\\s*=\\s*(\\w+\\.\\w+))(\\s+((?i)AND)\\s+((\\w+\\.\\w+)\\s*=\\s*(\\w+\\.\\w+)))*)\\s+(?=((\\bLEFT|RIGHT|OUTER|FULL\\b)\\s+)?JOIN|WHERE\\s+)", Pattern.CASE_INSENSITIVE);

    /**
     * 截取SQL的WHERE子句
     */
    private final static Pattern FIND_WHERE_CLAUSE_REG = Pattern.compile("\\bWHERE\\b[\\s\\S]*", Pattern.CASE_INSENSITIVE);

    /**
     * 获取每一条条件语句
     */
    private final static Pattern FIND_WHERE_CASE_REG = Pattern.compile("(?<=(WHERE|AND))((\\s+?\\w+\\.\\w+\\s+?\\bBETWEEN\\b.+?(\\bAND\\b).+?(?=(\\bAND\\b|$)))|(.+?(?=\\bAND\\b|$)))", Pattern.CASE_INSENSITIVE);

    /**
     * 获取连接类型
     */
    private final static Pattern FIND_JOIN_TYPE_REG = Pattern.compile("(?i)((\\bLEFT|RIGHT|OUTER|FULL\\b)\\s+)?\\bJOIN\\b", Pattern.CASE_INSENSITIVE);

    private final static String FIELD_REG = "\\b\\w+\\.\\w+\\b";

    private final static Pattern IS_INNER_CONDITION_REG = Pattern.compile(FIELD_REG + "\\s*(=|<|<=|>|>=|!=)\\s*(" + FIELD_REG + ")", Pattern.CASE_INSENSITIVE);

    private final static Pattern FIND_ORDERED_CLAUSE = Pattern.compile("((?i)ORDER\\s+BY(\\s*\\w+\\.\\w+\\s*(\\bDESC|ASC\\b)?(,\\s*)?)+)", Pattern.CASE_INSENSITIVE);

    private final static Pattern FIND_LIMIT_CLAUSE = Pattern.compile("((?i)\\bLIMIT\\s+\\d+)", Pattern.CASE_INSENSITIVE);


    private final static Pattern FIND_ORDERED_FIELD_REG = Pattern.compile("\\w+\\.\\w+(\\s+DESC|ASC)?", Pattern.CASE_INSENSITIVE);

    private final static Pattern FIND_SELECT_CLAUSE_REG = Pattern.compile("(?<=\\bSELECT\\b).*?(?=FROM)", Pattern.CASE_INSENSITIVE);

    private final static Pattern FIELD_DESC_REG_0 = Pattern.compile("(?i)\\w+\\.\\w+\\s+(AS\\s+)?\\S+");
    private final static Pattern FIELD_DESC_REG_1 = Pattern.compile("(?i)\\w+\\.\\w+\\s+(AS\\s+)?('.*?')");
    private final static Pattern FIELD_DESC_REG_2 = Pattern.compile("(?i)\\w+\\.\\w+\\s+(AS\\s+)?(\".*?\")");

    private final static Pattern FIND_STR_IN_SINGLE_QUOTE = Pattern.compile("(?<=').*?(?=')");
    private final static Pattern FIND_STR_IN_DOUBLE_QUOTE = Pattern.compile("(?<=\").*?(?=\")");
    private final static Pattern FIND_STR_IN_PARENTHESES = Pattern.compile("(?<=\\().*(?=\\))");

    @Getter
    private final String sql;

    private List<String> tables;

    public CrossSourceSQLParser(String sql) {
        Objects.requireNonNull(sql);
        sql = sql.replaceAll("[\\t\\n\\r\\f]", " ").trim();
        if (sql.charAt(sql.length() - 1) == ';')
            sql = sql.substring(0, sql.length() - 1);
        this.sql = sql;
        if (!this.sql.matches(SQL_FORMAT_REG.pattern())) {
            throw new IllegalArgumentException("wrong sql format: " + this.sql);
        }
    }

    /**
     * 获取sql中涉及的表
     *
     * @return 查询语句所连接的表
     */
    public List<String> getTables() {
        if (tables == null) {
            synchronized (this) {
                if (tables == null) {
                    Matcher matcher = FIND_TABLE_REG.matcher(sql);
                    List<String> tables = new ArrayList<>(4);
                    while (matcher.find()) {
                        String str = matcher.group();
                        tables.add(str.trim());
                    }
                    this.tables = tables;
                }
            }
        }
        return tables;
    }

    public List<String> getJoinCondition() {
        Matcher matcher = FIND_JOIN_FIELD_REG.matcher(sql);
        List<String> joinConditions = new ArrayList<>();
        while (matcher.find()) {
            String line = matcher.group();
            log.debug(line);
            Matcher innerMatcher = SPLIT_JOIN_CONDITION_REG.matcher(matcher.group());
            while (innerMatcher.find()) {
                joinConditions.add(innerMatcher.group().trim());
            }
        }
        return joinConditions;
    }

    // 解析sql的表关系
    public LinkRelation analysisRelation() {
        LinkRelation linkRelation = new LinkRelation();
        Matcher selectClauseMatcher = FIND_SELECT_CLAUSE_REG.matcher(sql);
        if (selectClauseMatcher.find()) {
            String selectClause = selectClauseMatcher.group();
            String[] fieldDescStrs = selectClause.split(",");
            if (!(fieldDescStrs.length == 1 && Objects.equals(fieldDescStrs[0].trim(), "*"))) {
                List<Pair<String, String>> outputFields = new ArrayList<>();
                for (String fieldStr : fieldDescStrs) {
                    fieldStr = fieldStr.trim();
                    if(fieldStr.matches("\\w+\\.((\\w+)|\\*)")){
                        outputFields.add(Pair.of(fieldStr, fieldStr));
                        continue;
                    }
                    if(fieldStr.matches(FIELD_DESC_REG_0.pattern())){
                        String[] splitFieldStr = fieldStr.split("(?i)\\s+(AS\\s+)?");
                        outputFields.add(Pair.of(splitFieldStr[0].trim(), splitFieldStr[1].trim()));
                        continue;
                    }
                    if(fieldStr.matches(FIELD_DESC_REG_1.pattern())){
                        outputFields.add(Pair.of(fieldStr.split("(?i)\\s+(AS\\s+)?")[0].trim(), fieldStr.substring(fieldStr.indexOf("'") + 1, fieldStr.lastIndexOf("'"))));
                        continue;
                    }
                    if(fieldStr.matches(FIELD_DESC_REG_2.pattern())){
                        outputFields.add(Pair.of(fieldStr.split("(?i)\\s+(AS\\s+)?")[0].trim(), fieldStr.substring(fieldStr.indexOf("\"") + 1, fieldStr.lastIndexOf("\""))));
                        continue;
                    }
                    throw new IllegalArgumentException("unknown filed description: " + fieldStr);
                }
                linkRelation.setOutputDescription(outputFields);
            }
            log.debug("select clause: {}", selectClause);
        } else {
            throw new IllegalArgumentException("not found select clause");
        }
        Matcher findFirstTableMatcher = FIND_FIRST_TABLE_REG.matcher(sql);
        List<String> tables = new ArrayList<>(4);
        Map<String, LinkRelation.TableNode> virtualNodeMap = new HashMap<>();
        // k: 表别名, v: 原表全名
        Map<String, String> tableNameRef = new HashMap<>(4);

        if (findFirstTableMatcher.find()) {
            String str = findFirstTableMatcher.group();
            String[] strings = splitSQLTable(str);
            String prevTable = strings[1];
            tables.add(prevTable);
            virtualNodeMap.put(prevTable, new LinkRelation.TableNode(prevTable));
            tableNameRef.put(strings[1], strings[0]);
        } else {
            throw new IllegalArgumentException("not found first table in sql");
        }

        Matcher matcher = FIND_JOIN_CLAUSE_REG.matcher(sql);

        while (matcher.find()) {
            String joinSql = matcher.group().trim();
            JoinType joinType;
            Matcher findJoinTypeMatcher = FIND_JOIN_TYPE_REG.matcher(joinSql);
            log.debug("join clause: {}", joinSql);
            if (findJoinTypeMatcher.find()) {
                joinType = JoinType.of(findJoinTypeMatcher.group());
            } else {
                throw new IllegalArgumentException("could not found join type in clause: " + joinSql);
            }

            log.debug("join type: {}", joinType);

            Matcher innerMatcher = SPLIT_JOIN_CLAUSE_REG.matcher(joinSql);
            boolean isFirst = true;
            String currentTable = null;
            while (innerMatcher.find()) {
                String s = innerMatcher.group();
                log.debug(innerMatcher.group());
                if (isFirst) {
                    String[] strings = splitSQLTable(s);
                    currentTable = strings[1];
                    if (tableNameRef.containsKey(currentTable))
                        throw new IllegalArgumentException("table label name '" + currentTable + "' is duplicated");
                    tableNameRef.put(currentTable, strings[0]);
                    tables.add(currentTable);
                    virtualNodeMap.put(currentTable, new LinkRelation.TableNode(currentTable));
                    isFirst = false;
                    continue;
                }

                /*
                 * 将 "t0.f0 = t1.f0" 分割为: ["t0", "f0", "t1", "f0"]
                 * [0]和[2]为表名, [1]和[3]为对应字段名
                 *
                 */
                String[] strings = Stream.of(s.split("(=)|(\\.)")).map(String::trim).toArray(String[]::new);
                if (!(Objects.equals(currentTable, strings[0]) || Objects.equals(currentTable, strings[2]))) {
                    throw new IllegalArgumentException("join condition '" + s + "' not contains table " + currentTable);
                }
                LinkRelation.TableNode node0 = Objects.requireNonNull(virtualNodeMap.get(strings[0]), "bad condition: " + s + ", previous table not contain " + strings[0]);
                LinkRelation.TableNode node1 = Objects.requireNonNull(virtualNodeMap.get(strings[2]), "bad condition: " + s + ", previous table not contain " + strings[2]);
                // 由于是顺序解析, currentTable一定是当前已解析表名的最后一个, 其余表均在currentTable的前面
                if (node0.tableName().equals(currentTable)) {
                    // 若[0]对应了当前表的名称, 则node0作为右侧, node1作为左侧
                    addJoinField(node1, node0, strings[3], strings[1], joinType);
                } else {
                    // 若[1]对应了当前表的名称, 则node1作为右侧, node0作为左侧
                    addJoinField(node0, node1, strings[1], strings[3], joinType);
                }

            }
        }
        linkRelation.setTableLabelRef(tableNameRef);
        linkRelation.setVirtualNodeMap(virtualNodeMap);
        linkRelation.setTables(tables);
        return linkRelation;
    }

    private String[] splitSQLTable(String str) {
        return Stream.of(str.trim().split("\\s+((?i)AS\\s+)?"))
                .map(String::trim)
                .toArray(String[]::new);
    }

    private void addJoinField(LinkRelation.TableNode left, LinkRelation.TableNode right, String leftField, String rightField, JoinType joinType) {
        JoinProfile profile0 = left.next().computeIfAbsent(right.tableName(), k -> new JoinProfile(joinType));
        String leftFieldFullName = left.tableName() + "." + leftField;
        String rightFieldFullName = right.tableName() + "." + rightField;

        if (!Objects.equals(profile0.joinType(), joinType)) {
            throw new IllegalArgumentException(String.format("left table '%s' an right table '%s' is '%s', but condition '%s=%s' is '%s'",
                    left.tableName(), right.tableName(), profile0.joinType(), leftFieldFullName, rightFieldFullName, joinType));
        }
        profile0.joinFields().add(Pair.of(leftFieldFullName, rightFieldFullName));

        JoinProfile profile1 = right.prev().computeIfAbsent(left.tableName(), k -> new JoinProfile(joinType));
        if (!Objects.equals(profile1.joinType(), joinType)) {
            throw new IllegalArgumentException(String.format("left table '%s' an right table '%s' is '%s', but condition '%s=%s' is '%s'",
                    left.tableName(), right.tableName(), profile1.joinType(), leftFieldFullName, rightFieldFullName, joinType));
        }
        profile1.joinFields().add(Pair.of(leftFieldFullName, rightFieldFullName));

    }

    public QueryCondition analysisWhereCondition() {
        Matcher matcher = FIND_WHERE_CLAUSE_REG.matcher(sql);
        QueryCondition queryCondition = new QueryCondition();
        Map<String, List<String>> cases = new HashMap<>();
        List<ConditionItem> innerCase = new ArrayList<>();
        if (matcher.find()) {
            String whereClause = matcher.group();
            log.debug("where clause: {}", whereClause);
            queryCondition.sqlWhereClause(whereClause.replaceAll("(?i)where", "").trim());
            Matcher orderedMatcher = FIND_ORDERED_CLAUSE.matcher(whereClause);
            if (orderedMatcher.find()) {
                String orderedClause = orderedMatcher.group();
                log.info("order clause: {}", orderedClause);
                Matcher orderedFieldMatcher = FIND_ORDERED_FIELD_REG.matcher(orderedClause);
                List<Pair<String, Boolean>> orderedField = new ArrayList<>();
                while (orderedFieldMatcher.find()) {
                    String str = orderedFieldMatcher.group().trim();
                    if (str.matches(".*(?i)desc\\s*$")) {
                        orderedField.add(Pair.of(str.split("\\s+")[0], false));
                    } else
                        orderedField.add(Pair.of(str.split("\\s+")[0], true));
                }
                log.debug("order by: {}", orderedField);
                queryCondition.orderedFields(orderedField);
                whereClause = whereClause.replaceAll(FIND_ORDERED_CLAUSE.pattern(), "").trim();
            }
            Matcher limitMatcher = FIND_LIMIT_CLAUSE.matcher(whereClause);
            if (limitMatcher.find()) {
                String limitCount = limitMatcher.group().replaceAll("(?i)limit\\s+?", "").trim();
                log.debug("limit: {}", limitCount);
                queryCondition.limit(Integer.parseInt(limitCount));
                whereClause = whereClause.replaceAll(FIND_LIMIT_CLAUSE.pattern(), "").trim();
            }
            Matcher caseMatcher = FIND_WHERE_CASE_REG.matcher(whereClause);
            while (caseMatcher.find()) {
                String whereCase = caseMatcher.group().trim();
                String tableName = whereCase.substring(0, whereCase.indexOf('.'));
                log.debug("where case: {}", whereCase);
                if (whereCase.matches(IS_INNER_CONDITION_REG.pattern())) {
                    String[] splitCase = whereCase.split("(=|<|<=|>|>=|!=)");
                    String rightPart = splitCase[1].trim();
                    ConditionItem conditionItem = new ConditionItem();
                    conditionItem.leftField(splitCase[0]);
                    if (rightPart.matches("\\w+\\.\\w+")) {
                        conditionItem.rightFields(new String[]{splitCase[1]});
                        conditionItem.rightFields(new String[]{rightPart});
                    } else
                        throw new IllegalArgumentException("not support: " + whereCase);

                    innerCase.add(conditionItem);
                } else {
                    cases.computeIfAbsent(tableName, k -> new ArrayList<>()).add(whereCase);
                }
            }
        } else {
            throw new IllegalArgumentException("not found where clause");
        }
        log.debug(cases.toString());
        queryCondition.conditions(cases);
        queryCondition.crossSourceCondition(innerCase);
        return queryCondition;
    }

}
