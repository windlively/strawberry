package ink.andromeda.strawberry.test;

import ink.andromeda.strawberry.core.CrossOriginSQLParser;
import ink.andromeda.strawberry.core.VirtualRelation;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.util.StopWatch;

import static ink.andromeda.strawberry.tools.GeneralTools.toJSONString;

@Slf4j
public class StaticTest {

    @Test
    public void sqlRegxTest(){
        String sql = " SELECT * FROM s1.d1.t1 AS t1\n" +
                     " JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4\n" +
                     " JOIN s2.d2.t3 AS t3 ON t2.f2 = t3.f3 AND t2.f1 = t3.f1\n" +
                     " WHERE t1.f2 = 'xxx' AND t1.f2 > 'xxx' AND t3.f1 IN ('xxx', 'xxx') AND t1.f3 BETWEEN 'xxx' AND 'xxx';";
        CrossOriginSQLParser sqlParser = new CrossOriginSQLParser(sql);
        System.out.println(sqlParser.getTables());
        System.out.println(sqlParser.getJoinCondition());
    }

    @Test
    public void sqlAnalysisTest(){
        String sql = " SELECT * FROM s1.d1.t1 AS t1\n" +
                     " JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4\n and t1.f2 = t2.f3" +
                     " JOIN s2.d2.t3 AS t3 ON t2.f2 = t3.f3 AND t2.f1 = t3.f1 and t3.f5=t1.f1\n" +
                     " WHERE t1.f2 = 'xxx' AND t1.f2 > 'xxx' AND t3.f1 IN ('xxx', 'xxx') AND t1.f3 BETWEEN 'xxx' AND 'xxx' AND t2.f3 LIKE '%s' AND t2.f1 between '-1' and '000';";
        StopWatch stopWatch = new StopWatch("sql analysis");
        stopWatch.start();
        CrossOriginSQLParser sqlParser = new CrossOriginSQLParser(sql);
        VirtualRelation relation = sqlParser.analysis();
        stopWatch.stop();
        log.info(stopWatch.prettyPrint());
        log.info(toJSONString(relation));

    }

}
