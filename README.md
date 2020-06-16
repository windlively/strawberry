# strawberry

#### 介绍
一个MYSQL跨库查询工具  
PS: 虽然MySQL有跨库查询引擎，但是需要修改数据库引擎，用法麻烦，且对原有数据库性能影响较大，在真实业务环境下几乎无法使用，我也未能找到Java版类似的工具，所以项目应运而生。

语法:
```mysql
SELECT ta.f1 AS 字段1, tc.f1 '字段2', tb.f2 字段8, td.* 
  FROM test.test.join_test_a ta
  RIGHT JOIN test.test.join_test_b tb ON ta.f1=tb.f2 AND ta.f1=tb.f1 
  RIGHT JOIN test.test.join_test_a td ON td.f1=tb.f2 
  LEFT JOIN test.test.join_test_c tc ON tc.f1=tb.f1 
  WHERE tc.f1 BETWEEN 0 AND 1 ORDER BY tb.f2 DESC , tc.f3 ASC, tc.f3 LIMIT 2
```

说明:
- 表名必须带数据源名称, 否则语法检查不通过, 因为在跨数据源时可能出现数据库重名情况, 数据源名称为在配置文件中定义的pool-name
- 表明必须定义别名, 否则语法检查不通过, 解决一张表多次JOIN问题, 同时避免SQL语句过长
- JOIN条件一般为原生表的有索引字段，否则查询缓慢
- 查询条件为不同表的两个字段时, 暂只支持&lt;, &le;, &gt;, &ge;, =操作符
- 查询条件仅支持AND关系, 或者OR两边为相同表的条件
- 当SQL仅作为一个表结构描述, 而查询条件经常变化的场景下, 可使用*VirtualDataSet*实例化SQL表结构, 避免SQL结构重复解析的耗时

#### 使用
- 可单独使用, 提供获取数据源以及表元信息的Function即可
- 使用spring boot自动配置, *MetaInfoDao*组件提供获取数据源以及表元信息的方法
    ```yaml
      strawberry:
        enable: true # 是否启用自动配置, 默认true
       # 数据源列表, 每一项可参考Hikari配置的项目, pool-name必须提供, 即数据源名称
        data-source-list:
         - pool-name: master
           driver-class-name: com.mysql.cj.jdbc.Driver
           jdbc-url: jdbc:mysql://127.0.0.1:3306/
           password: 4444
           username: root
           connection-test-query: SELECT 1
           validation-timeout: 18800
         - pool-name: test
           driver-class-name: com.mysql.cj.jdbc.Driver
           jdbc-url: jdbc:mysql://127.0.0.1:3306/?serverTimezone=Asia/Shanghai
           password: 58603924715
           username: root
           connection-test-query: SELECT 1
           validation-timeout: 18800
    ```

