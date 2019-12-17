# 自定义 Spring Security 功能
## 使用软件的版本
|软件名称|软件版本|
|----|----|
|MySQL|5.7|
|spring-boot|2.1.9.RELEASE|

## 自定义数据库模型的认证与授权
在spring-security框架中，提供了两种不同的认证与授权方式:基于内存、基于数据库。
在spring-security源码中有创建表的代码(位置为:spring-security/core/src/main/resources/org/springframework/security/core/userdetails/jdbc/users.ddl，版本为5.1.6.RELEASE):
```sql
create table users(username varchar_ignorecase(50) not null primary key,password varchar_ignorecase(500) not null,enabled boolean not null);
create table authorities (username varchar_ignorecase(50) not null,authority varchar_ignorecase(50) not null,constraint fk_authorities_users foreign key(username) references users(username));
create unique index ix_auth_username on authorities (username,authority);
```
如果有需要的话，也可以创建组，每个组有不同的权限:
```sql
create table group(id bigint primary key,group_name varchar_ignorecase(50) not null);
create table group_member(id bigint primary key,group_id bigint,username varchar_ignorecase(50) not null);
create table group_authorities(group_id bigint, authority varchar_ignorecase(50) not null);
```
相关的类:JdbcUserDetailsManager、JdbcDaoImpl。
在添加权限数据的时候，需要添加前缀ROLE_,在验证用户的权限的过程中，系统会自动添加响应的前缀。
```
# 文件位置:spring-security/core/src/main/java/org/springframework/security/core/userdetails/User.java
public UserBuilder roles(String... roles) {
    List<GrantedAuthority> authorities = new ArrayList<>(
            roles.length);
    for (String role : roles) {
        Assert.isTrue(!role.startsWith("ROLE_"), () -> role
                + " cannot start with ROLE_ (it is automatically added)");
        authorities.add(new SimpleGrantedAuthority("ROLE_" + role));
    }
    return authorities(authorities);
}
```
## 自定义Filter
在进行自定义Filter的过程中，需要实现Filter接口，然后在SpringSecurity的HttpSecurity配置中进行自定义，然后确定filter的执行顺序。
```java
package com.edu.springsecurity.filter;

import com.edu.springsecurity.exception.VerificationCodeException;
import com.edu.springsecurity.handler.CustomAuthenticationFailureHandler;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
//保证一次请求只会通过一次该filter
public class VerificationCodeFilter extends OncePerRequestFilter {

    private AuthenticationFailureHandler authenticationFailureHandler = new CustomAuthenticationFailureHandler();

    @Override
    protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, FilterChain filterChain) throws ServletException, IOException {
        // 非登录请求不校验验证码
        if (!"/login".equals(httpServletRequest.getRequestURI())) {
            filterChain.doFilter(httpServletRequest, httpServletResponse);
        } else {
            try {
                verificationCode(httpServletRequest);
                filterChain.doFilter(httpServletRequest, httpServletResponse);
            } catch (VerificationCodeException e) {
                authenticationFailureHandler.onAuthenticationFailure(httpServletRequest, httpServletResponse, e);
            }
        }
    }

    public void verificationCode (HttpServletRequest httpServletRequest) throws VerificationCodeException {
        String requestCode = httpServletRequest.getParameter("captcha");
        HttpSession session = httpServletRequest.getSession();
        String savedCode = (String) session.getAttribute("captcha");
        if (!StringUtils.isEmpty(savedCode)) {
            // 随手清除验证码，不管是失败还是成功，所以客户端应在登录失败时刷新验证码
            session.removeAttribute("captcha");
        }
        // 校验不通过抛出异常
        if (StringUtils.isEmpty(requestCode) || StringUtils.isEmpty(savedCode) || !requestCode.equals(savedCode)) {
            throw new VerificationCodeException();
        }
    }
}
```

## 自定义AuthenticationProvider
在AbstractUserDetailsAuthenticationProvider中实现了基本的认证流程，通过继承AbstractUserDetailsAuthenticationProvider，并实现retrieveUser和
additionalAuthenticationChecks两个抽象方法即可自定义核心认证过程，灵活性非常高。DaoAuthenticationProvider继承自AbstractUserDetailsAuthenticationProvider，
DaoAuthenticationProvider的用户信息来源于UserDetailsService，并且整合了密码编程的实现。所有的AuthenticationProvider包含的Authentication都来源于UsernamePasswordAuthenticationFilter。
```java
package com.edu.springsecurity.model;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.web.authentication.WebAuthenticationDetails;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

public class CustomWebAuthenticationDetails extends WebAuthenticationDetails {
    private String imageCode;
    private String savedImageCode;
    public String getImageCode() {
        return imageCode;
    }
    public String getSavedImageCode() {
        return savedImageCode;
    }
    // 补充用户提交的验证码和session保存的验证码
    public CustomWebAuthenticationDetails(HttpServletRequest request) {
        super(request);
        this.imageCode = request.getParameter("captcha");
        HttpSession session = request.getSession();
        this.savedImageCode = (String) session.getAttribute("captcha");
        if (!StringUtils.isEmpty(this.savedImageCode)) {
            // 随手清除验证码，不管是失败还是成功，所以客户端应在登录失败时刷新验证码
            session.removeAttribute("captcha");
        }
    }
}
```
```java
package com.edu.springsecurity.model;

import org.springframework.security.authentication.AuthenticationDetailsSource;
import org.springframework.security.web.authentication.WebAuthenticationDetails;

import javax.servlet.http.HttpServletRequest;

public class CustomWebAuthenticationDetailsSource implements AuthenticationDetailsSource<HttpServletRequest, WebAuthenticationDetails> {
    @Override
    public WebAuthenticationDetails buildDetails(HttpServletRequest context) {
        return new CustomWebAuthenticationDetails(context);
    }
}
```
```java
package com.edu.springsecurity.provider;

import com.edu.springsecurity.exception.VerificationCodeException;
import com.edu.springsecurity.model.CustomWebAuthenticationDetails;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class CustomAuthenticationProvider extends DaoAuthenticationProvider {
    public CustomAuthenticationProvider(UserDetailsService userDetailsService, PasswordEncoder passwordEncoder) {
        this.setUserDetailsService(userDetailsService);
        this.setPasswordEncoder(passwordEncoder);
    }
    @Override
    protected void additionalAuthenticationChecks(UserDetails userDetails, UsernamePasswordAuthenticationToken authentication) throws AuthenticationException {
        //根据用户提交的账号信息封装而来的UsernamePasswordAuthenticationToken。
        CustomWebAuthenticationDetails details = (CustomWebAuthenticationDetails) authentication.getDetails();
        String imageCode = details.getImageCode();
        String saveImageCode = details.getSavedImageCode();
        if (StringUtils.isEmpty(imageCode) || StringUtils.isEmpty(saveImageCode) || !imageCode.equals(saveImageCode)) {
            throw new VerificationCodeException();
        }
        super.additionalAuthenticationChecks(userDetails, authentication);
    }
}
```
## 自动登录
自动登录是将用户的信息保存在用户浏览器的cookie中，当用户下次访问时，自动实现校验并建立登录态的一种机制。
Spring Security提供了两种非常好的令牌:1.用散列算法加密用户必要的登录信息并生成令牌；2.数据库等持久性数据存储机制用的持久化令牌。
散列算法在Spring Security中是通过加密几个关键信息实现的:
```
hashInfo = md5hex(username+":"+expirationtime+":"+password+":"+key)
rememberCookie=base64(username+":"+expirationtime+":"+hashInfo)
```
expirationTime指本次自动登录的有效期，key为指定的一个散列盐值，用于防止令牌被修改。如果重启服务，key会重新生成，自动登录的cookie失效。

持久化令牌方案中，最核心的是series和token两个值，它们都是用MD5散列过的随机字符串。不同的是，series仅用户使用密码重新登录时更新，而token会在
每一个新的series中都会重新生成。这样可以解决散列加密方案中一个令牌可以同时在多端登录的问题。每个会话都会引发token的更新。其次，自动登录不会导致series
变更，而每次自动登录都需要同时验证series和token两值。
根据Spring Security中PersistenRememberMeToken，可以创建一个persisten_logins表(摘自JdbcTokenRepositoryImpl.java中):
```
create table persistent_logins (
    username varchar(64) not null, 
    series varchar(64) primary key, 
    token varchar(64) not null, 
    last_used timestamp not null)
```
## 注销登录
Spring Security默认注册了一个/logout路由，用户通过访问该路由可以安全地注销其登录状态，包含httpsession失效、清空已配置的Remember-me验证，以及清空
SecurityContextHolder，并在注销成功之后重定向到/login?logout页面。

## 会话管理
SessionManager是一个会话管理的配置器，其中，防御会话固定攻击的策略有四种:
>* none: 不做任何变动，登录之后沿用旧的session。
>* newSession: 登录之后创建一个新的session。
>* migrateSession(默认启用): 登录之后创建一个新的session，并将旧的session中的数据复制过来。
>* changeSessionId: 不创建新的会话，而使用由Servlet容器提供的会话固定保护。

默认情况下，会话在30分钟内没有活动便会失效。可以自定义session失效策略。
```java
package com.edu.springsecurity.strategy;
import org.springframework.security.web.session.InvalidSessionStrategy;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
public class CustomInvalidSessionStrategy implements InvalidSessionStrategy {
    @Override
    public void onInvalidSessionDetected(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        response.setContentType("application/json;charset=utf-8");
        response.getWriter().write("invalid session!");
    }
}
```
会话的时间最小为1分钟，如果设置数小于60秒，SpringBoot也会修改为60秒。
在Spring Security中，会话管理最完善的是会话并发控制(maximumSessions用于设置单个用户允许同时在线的最大会话数,如果没有额外配额,新登录的会话踢掉旧的会话)。
需要配置maxSessionPreventsLogin(true)。
使用HttpSessionListener接口监听Session相关事件，并在系统中注册该监听器。HttpSessionEventPublisher类中实现HttpSessionEventPublisher接口,
并转换为Spring的事件机制。
```
@Bean
public HttpSessionEventPublisher httpSessionEventPublisher(){
    return new HttpSessionEventPublisher();
}
```
Spring Security为了实现会话并发控制,采用会话信息表来管理用户的会话状态，具体实现见SessionRegistryImpl类。
principals采用了以用户信息为key的设计，如果使用自定义的UserDetails，需要自定义hashCode和equals。


集群会话的解决方法:
>* session保持:粘滞会话,采用IP哈希策略将相同客户端的请求妆发只相同服务器上进行处理。
>* session复制:服务器之间同步session数据,消耗数据贷款，占用大量的资源。
>* session共享:将session存储在独立的地方,在服务器之间共享,增加网络交互、数据容器的读/写性能、稳定性、网络I/O。

Spring Session就是专门用于解决集群会话问题。
```java
package com.edu.springsecurity.config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.security.web.session.HttpSessionEventPublisher;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.data.redis.config.annotation.web.http.EnableRedisHttpSession;
import org.springframework.session.security.SpringSessionBackedSessionRegistry;
import org.springframework.session.security.web.authentication.SpringSessionRememberMeServices;
@EnableRedisHttpSession
// @EnableSpringWebSession webflux 应用程序
public class HttpSessionConfig {
    @Bean
    public RedisConnectionFactory connectionFactory(){
        return new JedisConnectionFactory();
    }
    @Autowired
    private FindByIndexNameSessionRepository sessionRepository;
    //SpringSessionBackedSessionRegistry是session为spring security提供的
    //用于集群环境下控制会话并发的会话注册表实现类
    @Bean
    public SpringSessionBackedSessionRegistry sessionBackedSessionRegistry(){
        return new SpringSessionBackedSessionRegistry(sessionRepository);
    }
    //httpsession的事件监听,改用session提供的会话注册表
    @Bean
    public HttpSessionEventPublisher httpSessionEventPublisher(){
        return new HttpSessionEventPublisher();
    }
    @Bean
    public SpringSessionRememberMeServices rememberMeServices(){
        SpringSessionRememberMeServices rememberMeServices = new SpringSessionRememberMeServices();
        rememberMeServices.setAlwaysRemember(true);
        return rememberMeServices;
    }   
}
```
Redis存储用户session相关的数据，默认命名为spring:session。

## 跨域与CORS(跨站点资源分享)
浏览器解决跨域的方法有多种，包括jsonp、nginx转发和cors等。jsonp和cors都需要后端参与。
jsonp利用script标签可以实现跨域，只支持GET请求。
Access-Control-Allow-Origin:允许来自指定域请求。
Access-Control-Allow-Method:允许请求的方法(GET,POST)。
Access-Control-Allow-Headers:允许携带的头部字段。
Access-Control-Max-Age:本次预检请求的有效期，单位为秒。
Access-Control-Allow-Credentials:携带用户认证信息，Access-Control-Allow-Origin就需要指定url访问。

Spring Security在启用CORS之后，需要添加配置，设置哪些URL、Method可以支持跨域。
```
@Bean
public CorsConfigurationSource corsConfigurationSource(){
    CorsConfiguration corsConfiguration = new CorsConfiguration();
    //允许谷歌跨域
    corsConfiguration.setAllowedOrigins(Arrays.asList("http://wwww.google.com"));
    //允许使用GET方法和POST方法
    corsConfiguration.setAllowedMethods(Arrays.asList("GET","POST"));
    //允许携带凭证
    corsConfiguration.setAllowCredentials(true);
    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    //对所有URL生效
    source.registerCorsConfiguration("/**",corsConfiguration);
    return source;
}
```
## CSRF
防御CSRF的方式:
>* Http Referer:由浏览器添加的一个请求头字段，用于标识请求来源。
>* CsrfToken:系统分发一个CsrfToken，并记录下来，然后和用户的用户名、密码一块校验。

CSRF攻击完全是基于浏览器进行的。Spring Security通过注册一个CsrfFilter来专门处理CSRF攻击。
默认情况下，Spring Security加载的是一个HttpSessionCsrfTokenRepository。HttpSessionCsrfTokenRepository将CsrfToken值存储在HttpSession中,
并指定前段把CsrfToken值放入名为_csrf的请求参数或为X-CSRF-TOKEN的请求头字段里，前端必须用服务器渲染的方式注入CsrfToken值。
服务器端渲染，指的是后台语言通过一些模板引擎生成 html。浏览器端渲染，指的是用 js 去生成 html，前端做路由。

CookieCsrfTokenRepository是一种更加灵活可行的方案,它将CsrfToken存储在用户的cookie内。这样可以减少服务器HttpSession存储的内存消耗;
用cookie存储CsrfToken值时,前端可以用Javascript读取(需要设置该cookie的httpOnly属性为false)。
cookie只有在同域的情况下才能被读取。
LazyCsrfTokenRepository用来延时保存CsrfToken值(允许创建,但只有真正使用时才会被保存),没有独立使用一个csrfTokenRepository，而是专门用于
包裹其他csrfTokenRepository，默认包裹使用HttpSessionCsrfTokenRepository。LazyCsrfTokenRepository覆盖原先csrfTokenRepository的saveToken方法，
使得CsrfFilter中的saveToken方法失去实际的保存效果;然后修改了generateToken，使得CsrfToken在首次调用getToken时，才能真正调用saveToken方法对CsrfToken
进行保存。
从Spring Security 4.1.0.RELEASE的CsrfConfigure默认使用LazyCsrfTokenRepository。

## @EnableWebSecurity与过滤器链机制
@EnableWebSecurity是开启Spring Security的默认行为,通过@Import导入了WebSecurityConfiguration类(放入Spring的IOC容器里)。
Spring Boot自动配置web.xml。

## 参考文献
[Spring Session](https://docs.spring.io/spring-session/docs/current/reference/html5/#spring-security)
[服务器渲染和浏览器渲染的区别](https://blog.csdn.net/huangpb123/article/details/83592258)
[Spring Security 实战](https://book.douban.com/subject/34788867/)