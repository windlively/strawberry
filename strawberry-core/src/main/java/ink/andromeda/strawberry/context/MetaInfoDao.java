package ink.andromeda.strawberry.context;

import ink.andromeda.strawberry.core.DynamicDataSource;
import ink.andromeda.strawberry.entity.TableField;
import ink.andromeda.strawberry.entity.TableMetaInfo;
import ink.andromeda.strawberry.entity.po.ColumnPo;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.*;
import java.util.stream.Collectors;

import static ink.andromeda.strawberry.tools.GeneralTools.simpleQuery;
import static ink.andromeda.strawberry.tools.SQLTemplate.findColumnInfoSql;

@Service
public class MetaInfoDao {

    private final DynamicDataSource dynamicDataSource;

    private static final ThreadLocal<Map<String, TableMetaInfo>> TABLE_META_INFO_MAP =
            ThreadLocal.withInitial(WeakHashMap::new);

    public MetaInfoDao(DynamicDataSource dynamicDataSource) {
        this.dynamicDataSource = dynamicDataSource;
    }

    public void refreshTableMetaInfo(String tableFullName) {
        String[] strings = tableFullName.split("\\.");

        if (strings.length != 3) {
            throw new IllegalArgumentException("unrecognized table full name: "
                                               + tableFullName + ", which must like: {sourceName}.{schemaName}.{tableName}");
        }

        String sourceName = strings[0];
        String schemaName = strings[1];
        String tableName = strings[2];
        DataSource dataSource = getDataSource(sourceName);
        List<ColumnPo> columnPoList = simpleQuery(dataSource, findColumnInfoSql(schemaName, tableName),
                ColumnPo.class, true, true);

        TABLE_META_INFO_MAP.get().put(tableFullName, TableMetaInfo.builder()
                .fields(columnPoList.stream()
                        .map(c -> TableField.fromColumnPo(c, sourceName))
                        .collect(Collectors.toMap(TableField::name, e -> e)))
                .fieldList(columnPoList.stream().map(ColumnPo::columnName).collect(Collectors.toList()))
                .dataSource(dataSource)
                .name(tableName)
                .schemaName(schemaName)
                .sourceName(sourceName)
                .build());
    }

    public TableMetaInfo getTableMetaInfo(String tableFullName) {
        return Optional.ofNullable(TABLE_META_INFO_MAP.get().get(tableFullName)).orElseGet(() -> {
            refreshTableMetaInfo(tableFullName);
            return TABLE_META_INFO_MAP.get().get(tableFullName);
        });
    }

    public DataSource getDataSource(String sourceName) {
        return Objects.requireNonNull(dynamicDataSource.getDataSource(sourceName));
    }


}
