package ink.andromeda.strawberry.context;

import ink.andromeda.strawberry.entity.TableMetaInfo;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

@Service
public class DefaultQueryEnvironmentProvider implements QueryEnvironmentProvider{

    private final MetaInfoDao metaInfoDao;

    public DefaultQueryEnvironmentProvider(MetaInfoDao metaInfoDao) {
        this.metaInfoDao = metaInfoDao;
    }

    @Override
    public DataSource getDataSource(String sourceName) {
        return metaInfoDao.getDataSource(sourceName);
    }

    @Override
    public TableMetaInfo getTableMetaInfo(String tableFullName) {
        return metaInfoDao.getTableMetaInfo(tableFullName);
    }
}
