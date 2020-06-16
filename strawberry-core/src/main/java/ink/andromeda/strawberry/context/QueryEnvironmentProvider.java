package ink.andromeda.strawberry.context;

import ink.andromeda.strawberry.entity.TableMetaInfo;

import javax.sql.DataSource;

public interface QueryEnvironmentProvider {

    DataSource getDataSource(String sourceName);

    TableMetaInfo getTableMetaInfo(String tableFullName);

}
