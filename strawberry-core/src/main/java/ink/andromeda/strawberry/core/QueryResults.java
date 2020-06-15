package ink.andromeda.strawberry.core;

import lombok.Getter;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public final class QueryResults implements Iterable<Object[]>{

    @Getter
    private final String sql;

    private final List<Map<String, Object>> data;

    @Getter
    private final int size;

    @Getter
    private final String[] fields;

    @Getter
    private final String[] fieldLabels;

    public QueryResults(String sql, List<Map<String, Object>> data, String[] fields) {
        this(sql, data, fields, fields);
    }

    public QueryResults(String sql, List<Map<String, Object>> data, String[] fields, String[] fieldLabels) {
        this.sql = sql;
        this.data = Collections.unmodifiableList(data);
        Assert.isTrue(fields.length==fieldLabels.length, "field list length " + fields.length + " not equals field label list length " + fieldLabels.length);
        this.fields = fields;
        this.fieldLabels = fieldLabels;
        size = data.size();
    }

    @Override
    public String toString(){
        StringBuilder str = new StringBuilder();
        str.append("result table:\n");
        str.append(String.join("\t", fieldLabels)).append("\n");
        for (Object[] objects : this) {
            str.append(Stream.of(objects).map(Objects::toString).collect(Collectors.joining("\t")))
                    .append("\n");
        }
        return str.toString();
    }

    @Override
    public Iterator<Object[]> iterator() {
        Iterator<Map<String, Object>> iterator = data.iterator();
        return new Iterator<Object[]>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Object[] next() {
                Map<String, Object> map = iterator.next();
                return Stream.of(fields).map(map::get).toArray();
            }
        };
    }

    /**
     * 返回原始查询结果的一个副本
     *
     * @return 原始的查询数据
     */
    public List<Map<String, Object>> getPlainData(){
        return data.stream().map(HashMap::new).collect(Collectors.toList());
    }
}
