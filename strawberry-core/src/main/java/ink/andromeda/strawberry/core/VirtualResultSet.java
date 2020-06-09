package ink.andromeda.strawberry.core;

import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class VirtualResultSet {

    private final String sql;

    private final List<Map<String, Object>> data;

    private final Set<String> fields;

    @Override
    public String toString(){
        StringBuilder str = new StringBuilder();
        str.append("result table:\n");
        str.append(String.join("\t", fields)).append("\n");
        for (Map<String, Object> item : data){
            str.append(fields.stream().map(s -> Optional.ofNullable(item.get(s)).orElse("").toString()).collect(Collectors.joining("\t"))).append("\n");
        }
        return str.toString();
    }
}
