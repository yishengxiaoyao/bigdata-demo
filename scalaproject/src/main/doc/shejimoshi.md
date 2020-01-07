# Spring中用到的设计模式
Spring中用到的设计模式
|设计模式名称|举例||


Spring MVC的入口是DispatcherServlet，并实现了DispatcherServlet的init()方法，
在init()方法中完成IoC容器的初始化。

ApplicationContextAware接口，主要是通过侦听机制得到一个回调方法，从而得到IoC容器的上下文。

HandlerMapping是策略模式的应用，用输入URL间接调用不同的Method以达到获取结果的目的。

HandlerAdapter是适配器模式的应用，将Request的字符性参数自动适配为Method的Java实参,
主要实现参数列表自动适配和类型转换功能。

ViewResolver也算一种策略，根据不同请求选择不同的模版引擎来进行页面的渲染。

