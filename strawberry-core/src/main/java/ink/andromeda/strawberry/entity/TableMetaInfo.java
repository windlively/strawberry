package ink.andromeda.strawberry.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.sql.DataSource;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static ink.andromeda.strawberry.tools.GeneralTools.simpleQuery;
import static ink.andromeda.strawberry.tools.SQLTemplate.previewTableDataSql;

/**
 * 表结构定义
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableMetaInfo {

    private long id;

    // 源表所属库
    private String schema;

    // 源表名称
    private String name;

    /**
     * 列信息:
     * <p>列名<->列信息</p>
     */
    private Map<String, TableField> columns;

    /**
     * 该表对应的数据源
     */
    private DataSource dataSource;

    private DataSource dataSource(){
        return Objects.requireNonNull(dataSource);
    }

    public String fullName(){
        return schema + "." + name;
    }

    /**
     * 获取预览数据
     * @param limit 预览数据的长度
     * @return 预览数据
     */
    public List<Map<String, Object>> previewData(int limit){
        String sql = previewTableDataSql(fullName(), limit);
        return simpleQuery(dataSource(), sql);
    }

    public List<Map<String, Object>> previewData(){
        return previewData(20);
    }
}

