package ink.andromeda.strawberry.core;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.function.Function;

@Data
@Accessors(fluent = true)
public class ConditionItem {

    private String leftField;

    private String[] rightFields;

    private Operator operator;

    private Function<Object[], Object> rightFunction;

}
