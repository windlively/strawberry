package ink.andromeda.strawberry.tools;

public class SQLTemplate {

    private final static String PREVIEW_DATA_SQL_TEMPLATE = "SELECT * FROM %s LIMIT %s";

    private final static String FIND_COLUMN_INFO_SQL_TEMPLATE =
            "SELECT TABLE_NAME     as table_name,\n" +
            "       TABLE_SCHEMA   as schema_name,\n" +
            "       COLUMN_NAME    AS column_name,\n" +
            "       DATA_TYPE      AS column_type,\n" +
            "       COLUMN_COMMENT AS column_comment,\n" +
            "       IF(COLUMN_KEY IS NOT NULL AND COLUMN_KEY != '', COLUMN_KEY, NULL)     AS column_index,\n" +
            "       COLUMN_DEFAULT AS 'default_value'\n" +
            "FROM information_schema.COLUMNS\n" +
            "WHERE TABLE_SCHEMA = '%s'\n" +
            "  AND TABLE_NAME = '%s';";

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
