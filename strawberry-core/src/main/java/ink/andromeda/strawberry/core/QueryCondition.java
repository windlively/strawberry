package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.tools.Pair;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Accessors(fluent = true)
public class QueryCondition {

    @Getter
    @Setter
    private String sqlWhereClause;

    @Setter
    @Getter
    private Map<String, List<String>> conditions;

    @Setter
    @Getter
    private List<ConditionItem> crossSourceCondition;

    @Setter
    @Getter
    private List<Pair<String, Boolean>> orderedFields;

    @Setter
    @Getter
    private int limit = -1;

}
