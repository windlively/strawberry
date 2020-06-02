package ink.andromeda.strawberry.core;

import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.Objects;

@Service
public class OriginalInfoDao {

    private final DynamicDataSource dynamicDataSource;

    public OriginalInfoDao(DynamicDataSource dynamicDataSource) {
        this.dynamicDataSource = dynamicDataSource;
    }

    public void loadTableMetaInfo(String tableFullName){
        String[] strings = tableFullName.split("\\.");

        if(strings.length != 3){
            throw new IllegalArgumentException("unrecognized table full name: "
                                               + tableFullName + ", which must like: {sourceName}.{schemaName}.{tableName}");
        }

        String sourceName = strings[0];
        String schemaName = strings[1];
        String tableName = strings[2];
        DataSource dataSource = getDataSource(sourceName);


    }

    public DataSource getDataSource(String sourceName){
        return Objects.requireNonNull(dynamicDataSource.getDataSource(sourceName));
    }




}
