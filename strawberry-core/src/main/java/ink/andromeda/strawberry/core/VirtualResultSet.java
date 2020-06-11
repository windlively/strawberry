package ink.andromeda.strawberry.core;

import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class VirtualResultSet implements Iterable<Object[]>{

    @Getter
    private final String sql;

    private final List<Map<String, Object>> data;

    @Getter
    private final int size;

    @Getter
    private final String[] fields;

    public VirtualResultSet(String sql, List<Map<String, Object>> data, String[] fields) {
        this.sql = sql;
        this.data = Collections.unmodifiableList(data);
        this.fields = fields;
        size = data.size();
    }

    @Override
    public String toString(){
        StringBuilder str = new StringBuilder();
        str.append("result table:\n");
        str.append(String.join("\t", fields)).append("\n");
        for (Map<String, Object> item : data){
            str.append(Stream.of(fields)
                    .map(s -> Objects.toString(Optional.ofNullable(item.get(s)).orElse("nil")))
                    .collect(Collectors.joining("\t"))).append("\n");
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
}
