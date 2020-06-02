package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.core.JoinConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 跨数据源sql解析工具
 */
@Slf4j
public class CrossOriginSQLParser {

    /**
     * 校验SQL格式的正则, 所给的SQL语句必须为如下格式:
     * SELECT * FROM s1.d1.t1
     *   JOIN s1.d2.t2 ON s1.d1.t1.f1 = s1.d1.t2.f1 AND s1.d1.t1.f3=s1.d1.t2.f4
     *   JOIN s2.d2.t3 ON s1.d2.t2.f2=s1.d2.t3.f3 AND s1.d2.t2.f1 = s1.d2.t3.f1
     *   WHERE s1.d1.t1.f2 = 'xxx' AND s2.d2.t3.f2 > 'xxx' AND s2.d2.t3.f1 IN ('xxx', 'xxx') AND s1.d1.t1.f3 BETWEEN 'xxx' AND 'xxx';
     * (PS, sn: 数据源名称, dn: 数据库名称, tn: 表名, fn: 字段名)
     */
    private final static Pattern SQL_FORMAT_REG =
            Pattern.compile("((?i)SELECT)\\s+.+\\s+((?i)FROM)\\s+(\\w+(\\.\\w+){2})\\s+(\\s*(((?i)JOIN)\\s+(\\w+(\\.\\w+){2}))\\s+" +
                            "((?i)ON)\\s+((\\w+(\\.\\w+){3})\\s*=\\s*(\\w+(\\.\\w+){3}))\\s+(\\s*((?i)AND)\\s+((\\w+(\\.\\w+){3})\\s*=\\s*(\\w+(\\.\\w+){3})))*)+\\s+((?i)WHERE).*");

    /**
     * 获取表名的正则
     */
    private final static Pattern FIND_TABLE_REG =
            Pattern.compile("(((?<=((?i)FROM))\\s+(\\w+(\\.\\w+){2})\\s+(?=((?i)JOIN)))|((?<=(?i)JOIN)\\s+(\\w+(\\.\\w+){2})\\s+(?=((?i)ON))))");

    /**
     * 获取连接条件的表达式
     */
    private final static Pattern FIND_JOIN_FIELD_REG = Pattern.compile("(?<=\\s(ON))(\\s|\\S)+?(?=(\\s((JOIN)|(WHERE))\\s))", Pattern.CASE_INSENSITIVE);

    private final static Pattern SPLIT_JOIN_CONDITION_REG = Pattern.compile("(\\w+(\\.\\w+){3})\\s*=\\s*(\\w+(\\.\\w+){3})", Pattern.CASE_INSENSITIVE);

    private final String originalSql;

    private List<String> tables;

    public CrossOriginSQLParser(String originalSql) {
        Objects.requireNonNull(originalSql);
        if (!originalSql.matches(SQL_FORMAT_REG.pattern())) {
            throw new IllegalArgumentException("wrong sql format, " +
                                               "must like this format: SELECT * FROM s1.d1.t1 JOIN s1.d2.t2 JOIN s2.d2.t3 ON s1.d1.t1.f1 = s1.d1.t2.f1 AND s1.d2.t2.f1 = s1.d2.t3.f1 WHERE .......");
        }
        this.originalSql = originalSql.replaceAll("\\s+", " ");
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
                    Matcher matcher = FIND_TABLE_REG.matcher(originalSql);
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

    public List<String> getJoinCondition(){
        Matcher matcher = FIND_JOIN_FIELD_REG.matcher(originalSql);
        List<String> joinConditions = new ArrayList<>();
        while (matcher.find()){
            String line = matcher.group();
            log.info(line);
            Matcher innerMatcher = SPLIT_JOIN_CONDITION_REG.matcher(matcher.group());
            while (innerMatcher.find()){
                joinConditions.add(innerMatcher.group().trim());
            }
        }
        return joinConditions;
    }

    public void analysis(){
        Matcher matcher = FIND_TABLE_REG.matcher(originalSql);
        String prevTable = null;
        while (matcher.find()){
            String currentTable = matcher.group().trim();
            if(prevTable == null) {
                prevTable = currentTable;
                continue;
            }

            JoinConfig joinConfig = JoinConfig.builder()
                    .leftTable(prevTable)
                    .rightTable(currentTable)

                    .build();



        }

    }

}
