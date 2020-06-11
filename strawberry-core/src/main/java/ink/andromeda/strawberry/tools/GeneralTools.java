package ink.andromeda.strawberry.tools;

import com.google.gson.Gson;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.springframework.util.ReflectionUtils.*;

@Slf4j
public class GeneralTools {

    private final static ThreadLocal<Gson> gson = ThreadLocal.withInitial(Gson::new);

    public static Gson gsonInstance() {
        return gson.get();
    }

    // Spring的类型转换服务
    private final static ThreadLocal<DefaultConversionService> conversionService = ThreadLocal.withInitial(() -> {
        DefaultConversionService defaultConversionService = new DefaultConversionService();
        // String -> Date 转换器
        // defaultConversionService.addConverter(new StringToJavaDateConverter());
        return defaultConversionService;
    });

    public static DefaultConversionService conversionService() {
        return conversionService.get();
    }

    // 下划线字符串转驼峰式字符串
    public static String upCaseToCamelCase(String s, boolean bigCamelCase) {
        return separatorSegmentToCamelCase(s, '_', bigCamelCase);
    }

    // 驼峰转下划线
    public static String camelCaseToUpCase(String s) {
        return camelCaseToSeparatorSegment(s, '_');
    }

    /**
     * 分隔符分割转驼峰式
     *
     * @param s            原分隔符式字符串
     * @param separator    分隔符
     * @param bigCamelCase 是否为大驼峰
     */
    public static String separatorSegmentToCamelCase(String s, char separator, boolean bigCamelCase) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == separator) {
                res.append((char) (s.charAt(++i) - 32));
                continue;
            }
            if (i == 0 && bigCamelCase && ch > 90)
                res.append((char) (ch - 32));
            else
                res.append(ch);
        }
        return res.toString();
    }

    /**
     * 驼峰式转分隔符式
     *
     * @param s         原驼峰式字符串
     * @param separator 分隔符
     */
    public static String camelCaseToSeparatorSegment(String s, char separator) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch <= 90) {
                if (i > 0)
                    stringBuilder.append(separator);
                stringBuilder.append((char) (ch + 32));
                continue;
            }
            stringBuilder.append(ch);
        }
        return stringBuilder.toString();
    }

    /**
     * 为对象设置属性值
     *
     * @param config             配置
     * @param object             对象
     * @param configKeySeparator 配置的key的分隔符, 对应对象实例的驼峰式字段名称
     */
    public static void setBeanProperties(Map<String, Object> config, Object object, char configKeySeparator) {
        Class<?> clazz = object.getClass();
        config.forEach((k, v) -> {
            String methodName = "set" + separatorSegmentToCamelCase(k, configKeySeparator, true);
            Method method = findMethod(clazz, methodName, null);
            if (method != null)
                invokeMethod(method, object, conversionService().convert(v, method.getParameterTypes()[0]));
            else
                log.warn("config item [{}={}] maybe invalid, could not found [{}] method in class [{}]", k, v, methodName, clazz.getSimpleName());
        });
    }

    public static void testDataSourceConnection(DataSource dataSource) throws SQLException {
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute("SELECT 1");
        }
    }

    public static String subStringAt(String str, char startChar){
        return str.substring(str.indexOf(startChar) + 1);
    }

    // 数据库类型对应的Java类型
    public static Class<?> jdbcTypeToJavaType(String columnType) {
        switch (columnType) {
            case "varchar":
            case "text":
            case "char":
            case "longtext":
                return String.class;
            case "blob":
                return byte[].class;
            case "int":
            case "smallint":
            case "tinyint":
                return Integer.class;
            case "date":
            case "datetime":
            case "timestamp":
                return Date.class;
            case "bigint":
                return Long.class;
            case "decimal":
                return BigDecimal.class;
        }
        throw new IllegalArgumentException("unknown mysql column type: " + columnType);
    }

    public static String toJSONString(Object o) {
        return gsonInstance().toJson(o);
    }

    /**
     * 简单的SQL查询
     *
     * @param dataSource 数据源
     * @param sql        SQL语句
     * @return 数据视图
     */
    public static List<Map<String, Object>> simpleQuery(DataSource dataSource, String sql) {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery();

        ) {
            List<Map<String, Object>> result = new ArrayList<>(16);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> object = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    object.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                }
                result.add(object);
            }
            return result;
        } catch (SQLException ex) {
            // 在此处将异常处理掉, 否咋放在上层处理当发生异常时会导致connection未关闭, 一直处于挂起状态
            throw new IllegalStateException(String.format("exception in execute sql '%s', message: %s", sql, ex.toString()), ex);
        }
    }

    public static Map<String, Object> simpleQueryOne(DataSource dataSource, String sql) {
        List<Map<String, Object>> result = simpleQuery(dataSource, sql);
        if (result.size() == 0)
            return null;
        if (result.size() == 1)
            return result.get(0);
        throw new IllegalStateException(String.format("expect one result, but found %s now: %s", result.size(), sql));
    }

    public static <R> R simpleQueryOne(DataSource dataSource, String sql, Class<R> clazz, boolean autoTypeAliases, boolean fluentSetterMode) {
        List<R> result = simpleQuery(dataSource, sql, clazz, autoTypeAliases, fluentSetterMode);
        if (result.size() == 0)
            return null;
        if (result.size() == 1)
            return result.get(0);
        throw new IllegalStateException(String.format("expect one result, but found %s now: %s", result.size(), sql));
    }

    /**
     * @param dataSource       数据源
     * @param sql              SQL查询语句
     * @param clazz            返回的实体类类型
     * @param autoTypeAliases  是否自动驼峰类型转换
     * @param fluentSetterMode 是否为方法名和字段名一致的setter方法
     */
    public static <R> List<R> simpleQuery(DataSource dataSource, String sql, Class<R> clazz, boolean autoTypeAliases, boolean fluentSetterMode) {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery();
        ) {
            List<R> result = new ArrayList<>(16);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<Method> methodCache = new ArrayList<>(metaData.getColumnCount() + 1);
            List<Class<?>> methodParamTypeCache = new ArrayList<>(metaData.getColumnCount() + 1);
            Collections.addAll(methodCache, new Method[metaData.getColumnCount() + 1]);
            Collections.addAll(methodParamTypeCache, new Class<?>[metaData.getColumnCount() + 1]);
            // 预先加载对象的setter方法
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String originalName = metaData.getColumnLabel(i);
                String setterMethodName = fluentSetterMode ?
                        (autoTypeAliases ? upCaseToCamelCase(originalName, false) : originalName) :
                        ("set" + (autoTypeAliases ? upCaseToCamelCase(originalName, true)
                                : originalName.charAt(0) - 32 + originalName.substring(1)));
                Method method;
                if(fluentSetterMode){
                    method = Stream.of(getAllDeclaredMethods(clazz))
                            .filter(s -> s.getName().equals(setterMethodName) && s.getParameterCount() == 1)
                            .findFirst()
                            .orElse(null);
                }else {
                    method = findMethod(clazz, setterMethodName, null);
                }
                if (method == null) {
                    log.warn("could not found method {} in {}", setterMethodName, clazz.getTypeName());
                    continue;
                }
                if (method.getParameterCount() != 1) {
                    log.warn("method {} has multi parameters", setterMethodName);
                }
                methodCache.set(i, method);
                methodParamTypeCache.set(i, method.getParameterTypes()[0]);
            }
            while (resultSet.next()) {
                R object = clazz.getConstructor().newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    Method method = methodCache.get(i);
                    if (method != null) {
                        invokeMethod(method, object, conversionService().convert(resultSet.getObject(i), methodParamTypeCache.get(i)));
                    }
                }
                result.add(object);
            }
            return result;
        } catch (SQLException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException(String.format("exception in execute sql '%s', message: %s", sql, ex.toString()), ex);
        }
    }

    public static String javaObjectToSQLStringValue(Object object) {
        if (object == null)
            return "null";
        if (object instanceof String)
            return "'" + object + "'";
        if (object instanceof Number)
            return String.valueOf(object);
        if (object instanceof Date)
            return "'" + new DateTime(object).toString("yyyy-MM-dd HH:mm:ss.SSS") + "'";
        return object.toString();
    }


    public static void main(String[] args) {

    }

    public static DataSource buildDataSource(String dbName, String address, int port, String userName, String password) throws SQLException {
        Objects.requireNonNull(address);
        Assert.isTrue(port != 0, "data base port could not be 0!");
        Objects.requireNonNull(password);
        Objects.requireNonNull(dbName);
        Objects.requireNonNull(userName);
        String jdbcUrl = String.format(" jdbc:mysql://%s:%s/%s?zeroDateTimeBehavior=convertToNull", address, port, dbName);
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setJdbcUrl(jdbcUrl);
        hikariDataSource.setUsername(userName);
        hikariDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariDataSource.setPassword(password);
        hikariDataSource.setPoolName(dbName);
        hikariDataSource.setConnectionTimeout(TimeUnit.SECONDS.toMillis(15));
        testDataSourceConnection(hikariDataSource);
        return hikariDataSource;
    }

    /**
     * 转换两个对象, 从 src -> dest
     *
     * @param source     源对象
     * @param dest       要转换的对象
     * @param forceCover 当dest对象字段不为空时, 是否强行覆盖
     */
    public static <SRC, DEST> void copyFields(SRC source, DEST dest, boolean forceCover) {
        Class<?> destClass = dest.getClass();
        Class<?> sourceClass = source.getClass();
        // 仅处理当前对象的字段, 不处理继承的字段
        doWithLocalFields(sourceClass, field -> {
            makeAccessible(field);
            Object srcVal = getField(field, source);
            if (srcVal != null) {
                Field destField = findField(destClass, field.getName());
                if (destField != null) {
                    makeAccessible(destField);
                    Object destVal = getField(destField, dest);
                    if (destVal == null || forceCover) {
                        if (field.getType().equals(destField.getType()))
                            setField(destField, dest, srcVal);
                        else
                            setField(destField, dest, conversionService().convert(srcVal, destField.getType()));
                    }
                }
            }
        });
    }

    /**
     * @see #copyFields(Object, Object, boolean)
     */
    public static <SRC, DEST> void copyFields(SRC source, DEST dest) {
        copyFields(source, dest, false);
    }
}
