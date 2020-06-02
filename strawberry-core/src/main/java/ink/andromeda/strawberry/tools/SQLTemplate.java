package ink.andromeda.strawberry.tools;

public class SQLTemplate {

    private final static String PREVIEW_DATA_SQL_TEMPLATE = "SELECT * FROM %s LIMIT %s";

    private final static String FIND_COLUMN_INFO_SQL_TEMPLATE = "SELECT COLUMN_NAME AS col_name, DATA_TYPE AS col_type, COLUMN_COMMENT AS col_desc, (COLUMN_KEY IS NOT NULL AND COLUMN_KEY != '') AS is_index, COLUMN_DEFAULT AS 'default_value' FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";

    /**
     * 预览表数据的SQL
     */
    public static String previewTableDataSql(String tableName, int limit){
        return String.format(PREVIEW_DATA_SQL_TEMPLATE, tableName, limit);
    }

    public static String findColumnInfoSql(String schemaName, String tableName){
        return String.format(FIND_COLUMN_INFO_SQL_TEMPLATE, schemaName, tableName);
    }

}
