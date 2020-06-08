package ink.andromeda.strawberry.entity;

import ink.andromeda.strawberry.tools.GeneralTools;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 以具体值为索引的key, 数据需要顺序一致
 */
@Accessors(fluent = true)
public class IndexKey {

    @Getter
    private final Object[] values;

    public String toSQLString(){
        return Stream.of(values).map(GeneralTools::javaObjectToSQLStringValue)
                .collect(Collectors.joining(", "));
    }

    private IndexKey(Object[] values) {
        Objects.requireNonNull(values);
        this.values = values;
    }

    public static IndexKey of(Object... values) {
        return new IndexKey(values);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof IndexKey && Arrays.equals(values, ((IndexKey) o).values));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

}
