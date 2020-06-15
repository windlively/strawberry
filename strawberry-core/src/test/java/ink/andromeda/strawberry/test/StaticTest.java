package ink.andromeda.strawberry.test;

import ink.andromeda.strawberry.core.CrossSourceSQLParser;
import ink.andromeda.strawberry.core.LinkRelation;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.*;

import static ink.andromeda.strawberry.core.CrossSourceQueryEngine.$_LINE_NUMBER_STR;
import static ink.andromeda.strawberry.tools.GeneralTools.toJSONString;

@Slf4j
public class StaticTest {

    @Test
    public void sqlRegxTest(){
        String sql = " SELECT * FROM s1.d1.t1 AS t1\n" +
                     " JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4\n" +
                     " JOIN s2.d2.t3 AS t3 ON t2.f2 = t3.f3 AND t2.f1 = t3.f1\n" +
                     " WHERE t1.f2 = 'xxx' AND t1.f2 > 'xxx' AND t3.f1 IN ('xxx', 'xxx') AND t1.f3 BETWEEN 'xxx' AND 'xxx';";
        CrossSourceSQLParser sqlParser = new CrossSourceSQLParser(sql);
        System.out.println(sqlParser.getTables());
    }

    @Test
    public void sqlAnalysisTest(){
        String sql = " SELECT t1.t2 as fff223, t2.t4 fff1 , t3.* FROM s1.d1.t1 AS t1\n" +
                     " left JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4\n and t1.f2 = t2.f3" +
                     " right join s2.d2.t3 AS t3 ON  t2.f2 = t3.f3 AND t2.f1 = t3.f1 and t3.f5=t1.f1\n" +
                     " WHERE t1.f2 = 'xxx' AND t1.f2 > 'xxx' AND t3.f1 IN ('xxx', 'xxx') AND t1.f3 BETWEEN 'xxx' AND 'xxx' AND t2.f3 LIKE '%s' AND t2.f1 between '-1' and '000' and t1.f1 = t2.f3 " +
                     " order bY t2.f5, t3.f6 desc limit 12;";
        StopWatch stopWatch = new StopWatch("sql analysis");
        stopWatch.start();
        CrossSourceSQLParser sqlParser = new CrossSourceSQLParser(sql);
        LinkRelation relation = sqlParser.analysisRelation();
        sqlParser.analysisWhereCondition();
        stopWatch.stop();
        log.info(stopWatch.prettyPrint());
        log.info(relation.toString());
    }


    public static void main(String[] args) throws IOException {
        List<Integer> l1 = new ArrayList<>(10);
        for (int i = 0; i < 10000; i++) {
            l1.add(i);
        }
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            l1.set(5555,i);
        }
        long t2 = System.currentTimeMillis();

        List<Integer> l2 = new LinkedList<>();
        for (int i = 0; i < 10000; i++) {
            l2.add(i);
        }

        long t3 = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            l2.set(5555,i);
        }
        long t4 = System.currentTimeMillis();
        System.out.println(t2 - t1);
        System.out.println(t4 - t3);
    }

    @Test
    public void mapGetTest(){
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < 200; i++){
            map.put("aabb" + i, i);
        }
        map.put($_LINE_NUMBER_STR, 19);
        StopWatch stopWatch = new StopWatch("map test");
        stopWatch.start();
        Object[] objects = new Object[]{0, map};
        for (int i = 0; i < 100; i++){
            @SuppressWarnings("unchecked") Map<String, Object> m = (Map<String, Object>) objects[1];
            int k = (int) objects[0];
        }
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());
    }
}
