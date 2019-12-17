# xxl-job 更新
随着业务的开发，使用的Spring Boot的版本比较新，有时需要跑一些定时任务，然后搭建了一个xxl-job的服务。
但是xxl-job自身的Spring Boot的版本比较小，现需要更新。

## xxl-job Spring Boot 版本 更新
### 更新pom.xml
```pom
# xxl-job pom.xml
<properties>
    <spring.version>5.2.2.RELEASE</spring.version>
    <spring-boot.version>2.1.9.RELEASE</spring-boot.version>
    <mybatis-spring-boot-starter.version>2.1.1</mybatis-spring-boot-starter.version>
</properties>
```
### 更新properties文件
```properties
server.servlet.context-path=/xxl-job-admin
management.server.servlet.context-path=/actuator
```

## 搭建 xxl-job 服务

### 启动xxl-job服务
#### 数据准备
需要将xxl-job/doc/db/tables_xxl_job.sql的语句在相应的数据库中执行，我使用的是mysql，需要修改一下xxl-job配置的数据库的相关信息。
#### IDEA启动
将代码从github中拉取下来，然后启动IDEA的xxl-job-admin服务。
#### Maven启动
将代码从github中拉取下来，在命令行中转换到xxl-job-admin服务上,然后执行mvn spring-boot:run。

### 编写job-executor
#### 添加pom
```pom
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.edu.xxljob</groupId>
    <artifactId>xxl-job-executor-sample</artifactId>
    <version>1.0-SNAPSHOT</version>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>2.1.9.RELEASE</version>
    </parent>
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>
        <maven.compiler.encoding>UTF-8</maven.compiler.encoding>
        <java.version>1.8</java.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>com.xuxueli</groupId>
            <artifactId>xxl-job-core</artifactId>
            <version>2.1.3-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
    </dependencies>
</project>
```
### 编写xxl-job config
```java
package com.edu.xxljob.config;

import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class XxlJobConfig {
    private Logger logger = LoggerFactory.getLogger(XxlJobConfig.class);

    @Value("${xxl.job.admin.addresses}")
    private String adminAddresses;

    @Value("${xxl.job.executor.appname}")
    private String appName;

    @Value("${xxl.job.executor.ip}")
    private String ip;

    @Value("${xxl.job.executor.port}")
    private int port;

    @Value("${xxl.job.accessToken}")
    private String accessToken;

    @Value("${xxl.job.executor.logpath}")
    private String logPath;

    @Value("${xxl.job.executor.logretentiondays}")
    private int logRetentionDays;


    @Bean
    public XxlJobSpringExecutor xxlJobExecutor() {
        logger.info(">>>>>>>>>>> xxl-job config init.");
        XxlJobSpringExecutor xxlJobSpringExecutor = new XxlJobSpringExecutor();
        xxlJobSpringExecutor.setAdminAddresses(adminAddresses);
        xxlJobSpringExecutor.setAppName(appName);
        xxlJobSpringExecutor.setIp(ip);
        xxlJobSpringExecutor.setPort(port);
        xxlJobSpringExecutor.setAccessToken(accessToken);
        xxlJobSpringExecutor.setLogPath(logPath);
        xxlJobSpringExecutor.setLogRetentionDays(logRetentionDays);

        return xxlJobSpringExecutor;
    }
}
```
### 编写handler
```java
package com.edu.xxljob.handler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import org.springframework.stereotype.Component;

@JobHandler("UpdateSpringBootVersionHandler")
@Component
public class UpdateSpringBootVersionHandler extends IJobHandler {

    @Override
    public ReturnT<String> execute(String s) throws Exception {
        XxlJobLogger.log("UpdateSpringBootVersionHandler execute " + s);
        for (int i = 0; i < 10; i++) {
            XxlJobLogger.log("for each time: " + i);
        }
        return ReturnT.SUCCESS;
    }
}
```
### 编写配置文件
```properties
# web port
server.port=8081

# log config
logging.config=classpath:logback.xml


### xxl-job admin address list, such as "http://address" or "http://address01,http://address02"
xxl.job.admin.addresses=http://127.0.0.1:8080/xxl-job-admin

### xxl-job executor address
xxl.job.executor.appname=xxl-job-executor-sample
xxl.job.executor.ip=
xxl.job.executor.port=9999

### xxl-job, access token
xxl.job.accessToken=

### xxl-job log path
xxl.job.executor.logpath=/data/applogs/xxl-job/jobhandler
### xxl-job log retention days
xxl.job.executor.logretentiondays=30
```
### 启动xxl-job 服务
启动xxl-job服务之后，在浏览器地址框中输入http://127.0.0.1:8080/xxl-job-admin,用户名为:admin,密码为:123456。
在登录之后，可以自己增加新的执行器(需要指定ip)，也可以使用默认的。

在任务管理页面中，添加新的页面任务。

查看任务的日志。

查看具体的日志。
