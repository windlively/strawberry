package ink.andromeda.strawberry.test;

import ink.andromeda.strawberry.core.CrossOriginSQLParser;
import ink.andromeda.strawberry.core.VirtualRelation;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

import static ink.andromeda.strawberry.core.VirtualDataSet.$_LINE_NUMBER_STR;
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
                     " left JOIN s1.d2.t2 t2 ON t1.f1 = t2.f1 AND t1.f3 = t2.f4\n and t1.f2 = t2.f3" +
                     " right join s2.d2.t3 AS t3 ON  t2.f2 = t3.f3 AND t2.f1 = t3.f1 and t3.f5=t1.f1\n" +
                     " WHERE t1.f2 = 'xxx' AND t1.f2 > 'xxx' AND t3.f1 IN ('xxx', 'xxx') AND t1.f3 BETWEEN 'xxx' AND 'xxx' AND t2.f3 LIKE '%s' AND t2.f1 between '-1' and '000';";
        StopWatch stopWatch = new StopWatch("sql analysis");
        stopWatch.start();
        CrossOriginSQLParser sqlParser = new CrossOriginSQLParser(sql);
        VirtualRelation relation = sqlParser.analysis();
        stopWatch.stop();
        log.info(stopWatch.prettyPrint());
        log.info(toJSONString(relation));
    }


    public static void main(String[] args) throws IOException {
        String file = "C:\\Users\\白钧翰\\Desktop\\剔除异常数据后均值插补的修正数据.csv";
        String out = "C:\\Users\\白钧翰\\Desktop\\out.csv";
        List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.ISO_8859_1);
        List<List<String>> data = lines.stream().map(s -> Arrays.asList(s.split(","))).collect(Collectors.toList());
        List<String> header = Arrays.asList(lines.get(0).split(","));
        System.out.println(data);
        Map<String, Map<String, BigDecimal>> dataMap = new HashMap<>();
        for (int i = 1; i < header.size(); i++) {

            for (int j = 1; j < data.size(); j++) {
                String date = data.get(j).get(0);
                dataMap.computeIfAbsent(header.get(i), k -> new HashMap<>())
                        .put(date, new BigDecimal(data.get(j).get(i)));
            }

        }

        dataMap = dataMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().entrySet().stream().collect(Collectors.groupingBy(x -> x.getKey().substring(0, 6)))
                 .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, y -> BigDecimal.valueOf(y.getValue().stream().map(Map.Entry::getValue).mapToDouble(BigDecimal::floatValue).average().getAsDouble())))));

        List<String> months = new ArrayList<>(dataMap.get("T0000").keySet());
        months.sort(Comparator.comparing(Integer::parseInt));

        List<String> result = new ArrayList<>();
        result.add(String.join(",", header));
        for (int i = 0; i < months.size(); i++) {
            List<String> line = new ArrayList<>();
            for (int j = 0; j < header.size(); j++) {
                if(j == 0) {
                    line.add(months.get(i));
                    continue;
                }
                line.add(dataMap.get(header.get(j)).get(months.get(i)).toString());
            }
            result.add(String.join(",", line));
        }
        Files.write(Paths.get(out), result, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
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
