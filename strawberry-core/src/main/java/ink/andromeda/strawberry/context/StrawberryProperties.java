package ink.andromeda.strawberry.context;

import com.zaxxer.hikari.HikariConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "strawberry")
@ToString
public class StrawberryProperties {

    @Setter
    @Getter
    private boolean enable = true;

    @Setter
    @Getter
    private List<HikariConfig> dataSourceList;

}
