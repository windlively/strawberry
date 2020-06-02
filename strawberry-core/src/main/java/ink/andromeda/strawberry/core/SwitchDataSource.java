package ink.andromeda.strawberry.core;

import java.lang.annotation.*;

/**
 * 用于切换数据源的注解
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface SwitchDataSource {

    /**
     * 数据源类型, 在枚举类型中所定义的数据源
     *
     */
    String value() default "master";

    /**
     * 数据源名称, 此项的优先级高于{@link #value()}, 即当name不为空时, 则切换数据源至指定名称,
     * 忽略{@link #value()}所指定的值.
     */
    String name() default "";

}
