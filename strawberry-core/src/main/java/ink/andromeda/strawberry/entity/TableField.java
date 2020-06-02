package ink.andromeda.strawberry.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Objects;

@Getter
@Accessors(fluent = true)
public class TableField {

    private final String sourceName;

    private final String schemaName;

    private final String tableName;

    private final String name;

    private final String fullName;

    private final Class<?> javaType;

    private final String jdbcType;

    private Object value;

    private final boolean isIndex;

    private final String comment;

    @Builder
    private TableField(String sourceName, String schemaName, String tableName,
                       String name, Class<?> javaType, String jdbcType,
                       boolean isIndex, String comment) {
        this.name = name;
        this.javaType = javaType;
        this.jdbcType = jdbcType;
        this.isIndex = isIndex;
        this.comment = comment;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.sourceName = sourceName;
        fullName = String.join(".", sourceName, schemaName, tableName, name);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj instanceof TableField &&
                               (Objects.equals(((TableField) obj).sourceName, this.sourceName) &&
                                Objects.equals(((TableField) obj).schemaName, this.schemaName) &&
                                Objects.equals(((TableField) obj).tableName, this.tableName) &&
                                Objects.equals(((TableField) obj).name, this.name)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceName, schemaName, tableName, name);
    }
}
