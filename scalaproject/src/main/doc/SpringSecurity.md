# Spring Security 框架
## 1.身份认证和访问控制
应用程序安全可以分为两个独立的问题：身份认证和授权。Spring Security的架构将身份认证和授权分离，并为两者提供策略和扩展点。

### 1.1认证
AuthenticationManager是身份认证的主要策略接口，它只有一个方法。
```
public interface AuthenticationManager {
  Authentication authenticate(Authentication authentication)
    throws AuthenticationException;
}
```
在实现接口AuthenticationManager的authenticate()方法中，可以做下面三种事情：
>* 如果它可以验证输入是否为有效的主体，则返回一个身份验证的对象(通常情况下authenticated=true)
>* 如果确信输入不是有效的主体，抛出异常:AuthenticationException
>* 如果没有判断的情况下，返回null

AuthenticationException是一个运行时异常。通常情况下，不希望用户代码捕获并处理这个异常。

AuthenticationManager最常用的实现是ProviderManager，它委派了一串的AuthenticationProvider实例。
AuthenticationProvider与AuthenticationManager类似，然后提供了另外一个方法来查询是否支持这个身份认证类型。
```
public interface AuthenticationProvider {
	Authentication authenticate(Authentication authentication)
			throws AuthenticationException;
	boolean supports(Class<?> authentication);
}
```
在supports方法中，参数Class<?>实际上是Class<? extends Authentication>。
ProviderManager通过代理一连串的AuthenticationProviders来支持多种身份认证机制。
如果ProviderManager遇到无法识别的Authentication实例类型，将会跳过这个类型。

ProviderManager有一个可选的父类，如果提供者返回null，可以参考父类。
如果父类不可用，然后返回null，就会出现AuthenticationException。

有时，应用程序具有保护资源的逻辑组，并且每个组都可以拥有自己专用的AuthenticationManager。
通常情况下，各种ProviderManager公用一个父类，父类是一个全局资源，充当所有provider的后备资源。

### 1.2 Customizing Authentication Managers
Spring Security提供了一些配置帮助程序，可以快速获取应用程序中设置的常见验证管理管理器功能。
最常用的帮助程序是AuthenticationManagerBuilder，它非常适合设置内存、JDBC或LDAP用户详细信息，或者添加自定义的UserDetailsService。
配置应用程序全局AuthenticationManager的例子如下：
```
@Configuration
public class ApplicationSecurity extends WebSecurityConfigurerAdapter {
  @Autowired
  public initialize(AuthenticationManagerBuilder builder, DataSource dataSource) {
    builder.jdbcAuthentication().dataSource(dataSource).withUser("dave")
      .password("secret").roles("USER");
  }
}
```
AuthenticationManagerBuilder是自动注入(@Autowired)到一个对象(@Bean)的方法，这样可以创建一个全局的AuthenticationManager。
另外一种方式:
```
@Configuration
public class ApplicationSecurity extends WebSecurityConfigurerAdapter {
  @Autowired
  DataSource dataSource;

  @Override
  public configure(AuthenticationManagerBuilder builder) {
    builder.jdbcAuthentication().dataSource(dataSource).withUser("dave")
      .password("secret").roles("USER");
  }
}
```
重写父类的configure方法，AuthenticationManagerBuilder只能创建本地的AuthenticationManager，创建出来的对象是全局AuthenticationManager的一个子类。
在SpringBoot应用程序中，可以往bean中自动注入一个全局AuthenticationManager，如果使用本地的AuthenticationManager，需要将其暴露出来。

在SpringBoot应用程序中，如果用户没有提供自定义的AuthenticationManager，默认提供了一个全局的AuthenticationManager(只有一个用户)。

### 1.3身份认证或者访问控制
如认证成功之后，然后判断权限，这里面核心的策略是AccessDecisionManager。框架提供了三个实现，并且三个实现都委托给AccessDecisionVoter。

AccessDecisionVoter考虑了Authentication，并且使用ConfigAttributes来装饰安全对象。
```
boolean supports(ConfigAttribute attribute);

boolean supports(Class<?> clazz);

int vote(Authentication authentication, S object,
        Collection<ConfigAttribute> attributes);
```
安全对象(表示用户想要访问的资源)在AccessDecisionManager和AccessDecisionVoter的签名中完全通用。
ConfigAttribute是通用的，表示安全对象的装饰，其中包含一些元数据，用于确定访问它所需的权限级别。
ConfigAttribute是一个接口，只有一个通用的方法和返回一个字符串，返回的字符串中包含资源的所有者，需要有那些权限才能访问这个资源。
一个具体的ConfigAttribute对象是用户角色的名称，具有特定的格式(ROLE_ADMIN/ROLE_ADUIT)或表达需要评估的表达式。

大多数人只使用AffirmativeBased(如果没有选民拒绝，则授予访问权限)作为默认的AccessDecisionManager。
在定制化的时候，都是发生在选民上，例如添加新的规则，或者修改现有的规则。

在ConfigAttributes中使用SpEL表达式是非常常见的。AccessDecisionVoter可以处理SpEL表达式，并且可以为它们创建上下文。
如果需要自定义SpEL，可以实现SecurityExpressionRoot和SecurityExpressionHandler。

## 2.页面安全
Web层的Spring Security是基于Servlet Filters，这样有助于先查看Filters的作用。

客户端往app发送请求，tomcat根据请求的额uri来决定使用那些filter和servlet。
最多只有一个servlet处理一个请求，但是多个filter形成一个过滤链，并且filter有序，如果自己处理请求，可以跳过后面的filter。
filter可以修改下游filter/servlet中使用的request或者response。
filter chain中filter的顺序是非常重要的，SpringBoot提供了两种机制：a)使用FilterRegistrationBean设置filter;b)对filter使用注解Order来设置顺序。
一些现有的filter定义自己的常量，已表明各个filter的顺序。

Spring Security作为filterchain中一个filter来连接起来，其具体的类型是FilterChainProxy。
下面查看filter的顺序：


默认情况下，有11个filter。

security.basic.enabled=false  关闭