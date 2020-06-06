package ink.andromeda.strawberry.entity;

import java.util.Arrays;
import java.util.Objects;

/**
 * 以具体值为索引的key, 数据需要顺序一致
 */
public class IndexKey {

    private final Object[] values;

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
