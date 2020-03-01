# Spring 相关内容
自定义TypeFilter可以过滤一些不符合规则的bean，需要实现TypeFilter
 
Conditional按照要求注入bean 需要实现condition接口

Import 导入一系列组件

ImportSelector:返回符合要求的类，然后注入

ImportBeanDefinitionRegistry:导入自己自定义的一些类，手动注册

FactoryBean:工厂bean,返回对象:
>* 默认获取到的是工厂bean调用getObject闯将的对象
>* 要获取工厂bean本身需要添加&符号


Singleton:直接放入到容器中,在容器启动的时候创建对象，容器关闭的时候，销毁对象

prototype:每次获取的时候创建对象,容器不会销毁对象，需要手动指定销毁方法，来销毁对象，需要实现DisposableBean。

InitializingBean的实现afterPropertiesSet是在执行init-method之前执行。

PostConstructor:对象创建并赋值之后调用

PreDestroy:容器移除对象之前


## 容器注册组件
>* 包扫描/组件标注注解(@Controller/@Service/@Component/@Repository)
>* @Bean 导入第三方包里面的一个组件
>* @Import 快速给容器导入一个组件
>   * @Import(要导入到容器中的组件):容器中就会自动注册这个组件,id默认是全类名;
>   * @ImportSelector:返回需要导入的组件全类名数组;
>   * @ImportBeanDefinitionRegistrar:手动注册bean到容器中
>* 使用springt提供的FactoryBean(工厂bean)
>   * 默认获取到的工厂bean调用getObject创建的对象
>   * 获取工厂bean本身,需要在id前面添加一个&


## Bean的生命周期
bean的生命周期:
    bean的创建----初始化----销毁的过程
容器管理bean的生命周期:
可以自定义初始化和销毁方法,容器在bean进行到当前生命周期的时候来调用我们自定义的初始化和销毁方法

构造(对象创建):
    单实例:在容器启动时候创建对象
    多实例:在每次获取的时候创建对象

BeanPostProcessor.postProcessBeforeInitialization
初始化:
    对象创建完成,并赋值好,调用初始化方法
BeanPostProcessor.postProcessAfterInitialization
    
销毁:
    单实例:容器关闭的时候
    多实例:容器不会管理这个bean,容器不会调用销毁方法 
    
遍历得到容器中所有BeanPostProcessor,依次执行beforeInitialization,一旦返回null,跳出for循环,不会执行后面的BeanPostProcess。

populateBean  //给bean进行属性配置
initializeBean //
    postProcessBeforeInitialization //
    invokeInitMethod  执行init方法
    postProcessAfterInitialization

>* 指定初始化方法和销毁方法
>   * 指定init-method和destroy-method 
>* 通过bean实现InitializingBean(定义初始化逻辑)、DisposableBean(销毁逻辑)
>* 可以通过jsr250:
>   * @PostConstructor: 在bean创建完成b并且属性赋值完成,来执行初始化放啊
>   * @PreDestroy:在容器销毁bean之前通知我们j进行清理工作
>* BeanPostProcessor:bean的后置处理
>   * 在bean初始化前后进行一些处理工作
>* Spring 中BeanPostProcessor的使用
>   * ApplicationContextAwareProcessor为了注入IOC容器 
>   * BeanValidationPostProcessor数据校验
>   * InitDestroyAnnotationBeanPostProcessor用来处理PostConstructor和PreDestroy注解
>   * AutowiredAnnotationBeanPostProcessor 处理Autowired注解
>   * bean赋值,注入其他组件,@Autowired,生命周期注解功能,@Async等


## 自动装配

自动装配:
    Spring利用依赖注入(DI):完成对IOC容器中各个组件的依赖关系赋值:
>* @Autowired:自动注入
>   * 默认优先按照类型去容器中找到对应的组件:applicationContext.getBean(Book.class)
>   * 如果找到多个相同类型的组件,在将属性的名称作为组件的idq去容器中查找
>   * 多个的时候，可以使用@Qualifier来指定
>   * 自动装配一定要将属性赋值好,没有就会报错，可以设置属性不是必要的
>   * Primary: 让Springj进行自动装配的时候，默认使用首选的bean，也可以使用@Qualifier
>* @Resource(Java注解):
>   * 可以自动装配，默认按照组件名称进行装配的没有@primary和@qualifier功能
>* Autowired:
>   * 标注在方法位置:@Bean+方法参数:参数从容器中获取
>   * 标注在构造器上:如果组件只有一个有参构造函数,Autowired可以忽略
>   * 放在参数上:


## AOP
AOP动态代理
    指在程序运行期间将某段代码切入到指定方法指定位置的运行编程方式:

@EnableAspectJAutoProxy 开启基于注解AOP的模式


JoinPoint这个参数一定要出现在参数表的第一位。

@Bean标注的方法创建对象的时候,方法参数的值从容器中获取


>*  @EnableAspectJAutoProxy
>   * @Import(AspectJAutoProxyRegistrar.class)给容器导入AspectJAutoProxyRegistrar
>   * 利用AspectJAutoProxyRegistrar自定义给容器注册bean
>   * interAutoProxyCreator=AnnotationAwareAspectJAutoProxyCreator

AnnotationAwareAspectJAutoProxyCreator
    -> AspectJAwareAdvisorAutoProxyCreator
        -> AbstractAdvisorAutoProxyCreator
            -> AbstractAutoProxyCreator
                -> ProxyProcessorSupport


流程:
>* 传入配置类:创建IOC容器
>* 注册配置类:调用refresh容器
>* registerBeanPostProcessor:注册bean的后置处理器来方便拦截bean的创建;
>   * 先获取IOc容器已经定义了的需要创建对象所有BeanPostProcessor
>   * 给容器添加别的BeanPostProcessor
>   * 优先注册实现了PriorityOrdered接口的BeanPostProcessor。
>   * 注册实现Ordered接口的BeanPostProcessor。
>   * 注册没有优先级的BeanPostProcessor
>   * 注册BeanPostProcessor，就是将BeanPostProcessor的对象,放入到容器中
>   * 创建interAutoProxyCreator=AnnotationAwareAspectJAutoProxyCreator
>       * 创建bean的实例
>       * populateBean:给bean属性赋值
>       * initializeBean:初始化bean
>           * invokeAwareMethod:处理Aware接口的方法回掉
>           * 调用postProcessorBeforeInitialization
>           * invokeInitMethod:执行初始化方法
>           * 调用postProcessorAfterInitialization
>       * BeanPostProcessor处理
>   * 把BeanPostProcessor注册到BeanFactory中
>* finishBeanFactoryInitialization(BeanFactory);完成beanfactorych初始化工作,创建剩下的单实例bean
>   * 遍历容器中所有的bean,依次创建对象:getBean->doGetBean->getSingleton()
>   * 创建bean:
>       * 从缓存中获取当前bean,如果可以获取得到，直接返回，否则创建
>       * 创建好的bean都会被缓存起来
>       * createBean
>           * resolveBeforeInitialization:解析BeforeInitialization
>           * 希望后置处理再次返回一个代理对象如果能返回就是用，如果不能返回直接创建bean  
>
>
>* AnnotationAwareAspectJAutoProxyCreator
>   * 一个bean在创建之前,调用postProcessorBeforeInitialization
>       * 判断当前bean是否在adviseBean中
>       * 判断当前beans是否基础类型的Advice、PointCut、Advisor等
>       * 是否需要跳过
>           * 获取增强器集合:每一个封装的通知方法的增强器是InstantializationModelAwarePointCutAdvisor，判断是否为AspectJPointCutAdvisor,如果是，返回true
>           * 永远返回false
>  * 创建对象
>   postProcessorAfterInitialization:
>       * 获取当前bean的所有后置增强器:找到能在bean中使用的增强器;对增强器排序
>       * 保存bean到advicebeans中
>       * 如果当前bean有增强器,创建当前对象的bean代理
>           * 获取所有增强器
>           * 保存到ProxyFactory中
>           * 创建动态代理对象
>               * JDK动态代理
>               * cglib动态代理
>           * 给容器中返回当前组件的代理对象
>           * 容器中获取的是代理对象,执行的时候,代理对象就会执行通知方法的流程
>       * 目标方法执行
>           * 容器中保存了组件的代理对象,这个对象保存了详细信息
>               * 拦截目标方法的执行
>               * 根据ProxyFactory对象获取拦截器链
>               * 如果没有拦截器,直接执行方法
>               * 如果有拦截器,把需要执行的目标方法、目标对象、拦截器链等信息传入CglibMethodInvoation对象,并调用
>               * 拦截器的触发过程
>  


