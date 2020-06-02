package ink.andromeda.strawberry.core;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(StrawberryProperties.class)
@ComponentScan("ink.andromeda.strawberry")
public class StrawberryAutoConfiguration {




}
