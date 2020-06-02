package ink.andromeda.strawberry.test;

import ink.andromeda.strawberry.core.CrossOriginSQLParser;
import org.junit.Test;


public class StaticTest {

    @Test
    public void sqlRegxTest(){
        String sql = "SELECT * FROM s1.d1.t1\n" +
                     "         JOIN s1.d2.t2 ON s1.d1.t1.f1 = s1.d1.t2.f1 AND s1.d1.t1.f3=s1.d1.t2.f4 \n AND s1.d1.t1.f2=s1.d1.t2.f4" +
                     "         JOIN s2.d2.t3 ON s1.d2.t2.f2=s1.d2.t3.f3 AND s1.d2.t2.f1 = s1.d2.t3.f1\n" +
                     "         WHERE s1.d1.t1.f2 = 'xxx' AND s2.d2.t3.f2 > 'xxx' AND s2.d2.t3.f1 IN ('xxx', 'xxx') AND s1.d1.t1.f3 BETWEEN 'xxx' AND 'xxx'";
        CrossOriginSQLParser sqlParser = new CrossOriginSQLParser(sql);
        System.out.println(sqlParser.getTables());
        System.out.println(sqlParser.getJoinCondition());
    }

}
