package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.entity.TableField;
import ink.andromeda.strawberry.tools.Pair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Accessors(fluent = true)
public class JoinConfig {

    private String leftTable;

    private String rightTable;

    private String joinType;

    private Set<Pair<TableField, TableField>> joinFields = new HashSet<>();

    @Override
    public boolean equals(Object object) {
        if (this == object)
            return true;
        if (object instanceof JoinConfig) {
            return Objects.equals(leftTable, ((JoinConfig) object).leftTable)
                   && Objects.equals(rightTable, ((JoinConfig) object).rightTable);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftTable, rightTable);
    }
}
