package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.tools.Pair;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ink.andromeda.strawberry.tools.GeneralTools.toJSONString;

/**
 * 跨数据源sql解析工具
 */
@Slf4j
public class CrossOriginSQLParser {

    /**
     * 校验SQL格式的正则, 所给的SQL语句必须为如下格式:
     * <p>SELECT * FROM s1.d1.t1 AS t1
     * JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4
     * JOIN s2.d2.t3 AS t3 ON t2.f2 = t3.f3 AND t2.f1 = t3.f1
     * WHERE t1.f2 = 'xxx' AND t1.f2 > 'xxx' AND t3.f1 IN ('xxx', 'xxx') AND t1.f3 BETWEEN 'xxx' AND 'xxx';
     * (PS, sn: 数据源名称, dn: 数据库名称, tn: 表名, fn: 字段名)
     */
    private final static Pattern SQL_FORMAT_REG =
            Pattern.compile("\\s*((?i)SELECT)\\s+.+\\s+((?i)FROM)\\s+(\\w+(\\.\\w+){2})\\s+((?i)AS\\s+)?\\w+\\s+(\\s*(((?i)JOIN)\\s+(\\w+(\\.\\w+){2}))\\s+((?i)AS\\s+)?\\w+\\s+" +
                            "((?i)ON)\\s+((\\w+\\.\\w+)\\s*=\\s*(\\w+\\.\\w+))(\\s+((?i)AND)\\s+((\\w+\\.\\w+)\\s*=\\s*(\\w+\\.\\w+)))*)+\\s+((?i)WHERE).*");

    /**
     * 获取表名的正则
     */
    private final static Pattern FIND_TABLE_REG =
            Pattern.compile("(((?<=((?i)FROM))\\s+(\\w+(\\.\\w+){2})\\s+((?i)AS\\s+)?\\w+\\s+(?=((?i)JOIN)))|((?<=(?i)JOIN)\\s+(\\w+(\\.\\w+){2})\\s+((?i)AS\\s+)?\\w+\\s+(?=((?i)ON))))");

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
     *
     */
    private final static Pattern FIND_FIRST_TABLE_REG =
            Pattern.compile("(?<=(\\bFROM\\b))\\s+(\\w+(\\.\\w+){2})\\s+(AS\\s+)?\\w+\\s+(?=\\bJOIN\\b)", Pattern.CASE_INSENSITIVE);

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
            Pattern.compile("(((\\bJOIN\\b)\\s+(\\w+(\\.\\w+){2}))\\s+((?i)AS\\s+)?\\w+\\s+" +
                                                                        "((?i)ON)\\s+((\\w+\\.\\w+)\\s*=\\s*(\\w+\\.\\w+))(\\s+((?i)AND)\\s+((\\w+\\.\\w+)\\s*=\\s*(\\w+\\.\\w+)))*)\\s+(?=JOIN|WHERE\\s+)", Pattern.CASE_INSENSITIVE);

    /**
     * 截取SQL的WHERE条件部分
     */
    private final static Pattern FIND_WHERE_CLAUSE_REG = Pattern.compile("\\bWHERE\\b[\\s\\S]*", Pattern.CASE_INSENSITIVE);

    private final static Pattern FIND_WHERE_CASE_REG = Pattern.compile("(?<=(WHERE|AND))((\\s+?\\w+\\.\\w+\\s+?\\bBETWEEN\\b.+?(\\bAND\\b).+?(?=(\\bAND\\b|$)))|(.+?(?=\\bAND\\b|$)))", Pattern.CASE_INSENSITIVE);

    private final String sql;

    private List<String> tables;

    public CrossOriginSQLParser(String sql) {
        Objects.requireNonNull(sql);
        this.sql = sql.replaceAll("[\\t\\n\\r\\f]", " ");
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
            log.info(line);
            Matcher innerMatcher = SPLIT_JOIN_CONDITION_REG.matcher(matcher.group());
            while (innerMatcher.find()) {
                joinConditions.add(innerMatcher.group().trim());
            }
        }
        return joinConditions;
    }

    // 解析sql
    public VirtualRelation analysis() {
        Matcher findFirstTableMatcher = FIND_FIRST_TABLE_REG.matcher(sql);

        String prevTable;
        VirtualRelation virtualRelation = new VirtualRelation();
        List<String> fields = new ArrayList<>(32);
        Map<String, VirtualRelation.VirtualNode> virtualNodeMap = new HashMap<>();
        // k: 表别名, v: 原表全名
        Map<String, String> tableNameRef = new HashMap<>(4);

        if(findFirstTableMatcher.find()){
            String str = findFirstTableMatcher.group();
            String[] strings = splitSQLTable(str);
            prevTable = strings[1];
            virtualNodeMap.put(prevTable, new VirtualRelation.VirtualNode(prevTable));
            tableNameRef.put(strings[1], strings[0]);
        } else {
            throw new IllegalArgumentException("not found first table in sql");
        }

        Matcher matcher = FIND_JOIN_CLAUSE_REG.matcher(sql);

        while (matcher.find()) {
            String joinSql = matcher.group().trim();
            Matcher innerMatcher = SPLIT_JOIN_CLAUSE_REG.matcher(joinSql);
            boolean isFirst = true;
            String currentTable = null;
            while (innerMatcher.find()){
                String s = innerMatcher.group();
                log.info(innerMatcher.group());
                if(isFirst){
                    String[] strings = splitSQLTable(s);
                    currentTable = strings[1];
                    if(tableNameRef.containsKey(currentTable))
                        throw new IllegalArgumentException("table label name '" + currentTable + "' is duplicated");
                    tableNameRef.put(currentTable, strings[0]);
                    virtualNodeMap.put(currentTable, new VirtualRelation.VirtualNode(currentTable));
                    isFirst = false;
                    continue;
                }

                String[] strings = Stream.of(s.split("(=)|(\\.)")).map(String::trim).toArray(String[]::new);
                if(!(Objects.equals(currentTable, strings[0]) || Objects.equals(currentTable, strings[2]))){
                    throw new IllegalArgumentException("join condition '" + s +"' not contains table " + currentTable);
                };

                VirtualRelation.VirtualNode node0 = Objects.requireNonNull(virtualNodeMap.get(strings[0]), "bad condition: " + s + ", previous table not contain " + strings[0]);
                VirtualRelation.VirtualNode node1 = Objects.requireNonNull(virtualNodeMap.get(strings[2]), "bad condition: " + s + ", previous table not contain " + strings[2]);

                if(node0.tableName().equals(currentTable)){
                    addJoinField(node1, node0, strings[3], strings[1]);
                }else {
                    addJoinField(node0, node1, strings[1], strings[3]);
                }

            }
            prevTable = currentTable;
        }
        virtualRelation.setTableLabelRef(tableNameRef);
        virtualRelation.setVirtualNodeMap(virtualNodeMap);
        virtualRelation.setWhereCases(getWhereCase());
        return virtualRelation;
    }

    private String[] splitSQLTable(String str){
        return Stream.of(str.trim().split("\\s+((?i)AS\\s+)?"))
                .map(String::trim)
                .toArray(String[]::new);
    }

    private void addJoinField(VirtualRelation.VirtualNode left, VirtualRelation.VirtualNode right, String leftField, String rightField){
        left.next().computeIfAbsent(right.tableName(), k -> new ArrayList<>()).add(Pair.of(left.tableName() + "." + leftField, right.tableName() + "." + rightField));
        right.prev().computeIfAbsent(left.tableName(), k -> new ArrayList<>()).add(Pair.of(left.tableName() + "." + leftField, right.tableName() + "." + rightField));
    }

    private Map<String, List<String>> getWhereCase(){
        Matcher matcher = FIND_WHERE_CLAUSE_REG.matcher(sql);
        Map<String, List<String>> cases = new HashMap<>();
        if(matcher.find()){
            String whereClause = matcher.group();
            log.info("where clause: {}", whereClause);
            Matcher caseMatcher = FIND_WHERE_CASE_REG.matcher(whereClause);
            while (caseMatcher.find()){
                String whereCase = caseMatcher.group().trim();
                String tableName = whereCase.substring(0, whereCase.indexOf('.'));
                cases.computeIfAbsent(tableName, k -> new ArrayList<>()).add(whereCase);
                log.info("where case: {}", whereCase);
            }
        }else {
            throw new IllegalArgumentException("not found where clause");
        }
        log.info(cases.toString());
        return cases;
    }
}
