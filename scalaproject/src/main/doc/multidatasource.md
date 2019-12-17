# 多数据源配置
在使用Spring Boot开发的过程中，有时会需要多个数据库，就需要配置多个数据源。数据源的默认配置为spring.datasource.*。
## 配置文件配置
在配置多个数据源的时候,需要使用表示来区分数据源。
```pom
spring.datasource.primary.jdbc-url=jdbc:mysql://localhost:3306/db_one
spring.datasource.primary.username=root
spring.datasource.primary.password=123456
spring.datasource.primary.driver-class-name=com.mysql.jdbc.Driver
spring.datasource.secondary.jdbc-url=jdbc:postgresql://localhost:5432/db_two
spring.datasource.secondary.username=postgres
spring.datasource.secondary.password=123456
spring.datasource.secondary.driver-class-name=org.postgresql.Driver
```
## Java配置类
```java
package com.edu.datasource.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class DataSourceConfig {
	@Primary
	@Bean(name = "primaryDataSource")
	@Qualifier("primaryDataSource")
	@ConfigurationProperties(prefix = "spring.datasource.primary")
	public DataSource primaryDataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean(name = "secondaryDataSource")
	@Qualifier("secondaryDataSource")
	@ConfigurationProperties(prefix = "spring.datasource.secondary")
	public DataSource secondaryDataSource() {
		return DataSourceBuilder.create().build();
	}
}
```
## 配置连接池
```pom
spring.datasource.primary.maximumPoolSize=50
spring.datasource.primary.connectionTimeout=60000
spring.datasource.primary.maxLifetime=60000
spring.datasource.primary.idleTimeout=60000
spring.datasource.primary.validation-timeout=3000
spring.datasource.primary.loginTimeout=5
```
下面是摘自官网的说明:
```
Also, Hikari-specific metrics are exposed with a hikaricp prefix.
Each metric is tagged by the name of the Pool (can be controlled with spring.datasource.name).
```

## 源码查看
在Spring Boot的源码中，Hikaricp默认的连接池为10. 文件名字为:HikariConfig.java
```
private static final long CONNECTION_TIMEOUT = SECONDS.toMillis(30);
private static final long VALIDATION_TIMEOUT = SECONDS.toMillis(5);
private static final long IDLE_TIMEOUT = MINUTES.toMillis(10);
private static final long MAX_LIFETIME = MINUTES.toMillis(30);
private static final int DEFAULT_POOL_SIZE = 10;
```
由于Spring Boot2.0 开始时，默认的连接池为Hikari,需要查看 HikariDataSource.java、
DataSourceBuilder.java、DataSourceProperties.java。

## 参考文章
[Using multiple datasources with Spring Boot and Spring Data](https://medium.com/@joeclever/using-multiple-datasources-with-spring-boot-and-spring-data-6430b00c02e7)
[DataSource Metrics](https://docs.spring.io/spring-boot/docs/2.0.1.RELEASE/reference/htmlsingle/#production-ready-metrics-jdbc)
[Spring Boot - Multiple Data-Sources](https://blog.andresteingress.com/2017/10/13/spring-boot-data-sources.html)
[USING MULTIPLE DATA SOURCES IN SPRING BOOT AND JDBC](https://thecodingjourney.com/using-multiple-data-sources-in-spring-boot-and-jdbc/)
[How to Configure Multiple Data Sources in a Spring Boot Application](https://springframework.guru/how-to-configure-multiple-data-sources-in-a-spring-boot-application/)
[Multiple Data Sources with Spring Boot](https://www.javadevjournal.com/spring-boot/multiple-data-sources-with-spring-boot/)
