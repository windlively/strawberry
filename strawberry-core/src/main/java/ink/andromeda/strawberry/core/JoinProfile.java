package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.entity.JoinType;
import ink.andromeda.strawberry.entity.TableField;
import ink.andromeda.strawberry.tools.Pair;
import lombok.*;
import lombok.experimental.Accessors;
import org.springframework.util.Assert;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


@Accessors(fluent = true)
public class JoinProfile {

    @Getter
    private final JoinType joinType;

    @Getter
    private final Set<Pair<String, String>> joinFields = new HashSet<>();

    public JoinProfile(JoinType joinType) {
        Assert.notNull(joinType);
        this.joinType = joinType;
    }


    @Override
    public boolean equals(Object object) {
        return this == object ||
               object instanceof JoinProfile
               && joinFields.equals(((JoinProfile) object).joinFields)
                && Objects.equals(joinType, ((JoinProfile) object).joinType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, joinFields);
    }
}
