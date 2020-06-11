package ink.andromeda.strawberry.context;

import com.zaxxer.hikari.HikariDataSource;
import ink.andromeda.strawberry.core.DynamicDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static ink.andromeda.strawberry.tools.GeneralTools.testDataSourceConnection;

@Configuration
@EnableConfigurationProperties(StrawberryProperties.class)
@ComponentScan("ink.andromeda.strawberry")
@Slf4j
public class StrawberryAutoConfiguration {

    private final StrawberryProperties strawberryProperties;


    public StrawberryAutoConfiguration(StrawberryProperties strawberryProperties) {
        this.strawberryProperties = strawberryProperties;
        log.info(strawberryProperties.toString());
    }

    @Bean("dynamicDataSource")
    public DynamicDataSource dynamicDataSource(){
        Map<Object, DataSource> dataSourceMap = new HashMap<>();
        strawberryProperties.getDataSourceList().forEach(hikariConfig -> {
            String sourceName = Objects.requireNonNull(hikariConfig.getPoolName(), "source name(pool name) could be not null");
            try {
                HikariDataSource dataSource = new HikariDataSource(hikariConfig);
                testDataSourceConnection(dataSource);
                dataSourceMap.put(sourceName, dataSource);
            } catch (SQLException e) {
                log.error("data source '{}' is invalid: {}",sourceName,  e.toString(), e);
            }
        });
        DataSource defaultDataSource = Objects.requireNonNull(dataSourceMap.get("master"), "master data source is required");
        return new DynamicDataSource(defaultDataSource, dataSourceMap);
    }
}
