package ink.andromeda.strawberry.entity.po;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class ColumnPo {

    private String columnName;

    private String tableName;

    private String schemaName;

    private String columnType;

    private String columnComment;

    private String columnIndex;

    private String defaultValue;

}
