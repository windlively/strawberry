package ink.andromeda.strawberry.core;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@Aspect
@Component
@Slf4j
public class DataSourceDetermineAspect {

    private final DynamicDataSource dynamicDataSource;

    public DataSourceDetermineAspect(@Qualifier("dynamicDataSource") DynamicDataSource dynamicDataSource) {
        this.dynamicDataSource = dynamicDataSource;
    }

    // 动态切换数据源
    @Around("@annotation(SwitchDataSource)")
    public Object around(ProceedingJoinPoint joinPoint) {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        SwitchDataSource switchDataSource = method.getAnnotation(SwitchDataSource.class);
        String name = switchDataSource.value();
        // 如果name项不为空, 则使用name指定的数据源
        if(!"".equals(switchDataSource.name()))
            name = switchDataSource.name();
        dynamicDataSource.changeLookupKey(name);
        log.debug("use datasource {}", name);
        Object result = null;
        try {

            // 执行原方法
            result = joinPoint.proceed();

        } catch (Throwable throwable) {
            log.error("DataSourceDetermineAspect exception: {}", throwable.getMessage(), throwable);
            throwable.printStackTrace();
        }finally {
            // 重置为默认数据源
            dynamicDataSource.resetToDefault();
        }

        return result;
    }

}
