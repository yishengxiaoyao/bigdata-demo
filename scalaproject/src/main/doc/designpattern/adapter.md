# 适配器模式
适配器模式(Adapter Pattern) ：将一个接口转换成客户希望的另一个接口，适配器模式使接口不兼容的那些类可以一起工作，其别名为包装器(Wrapper)。
```
//设置适配器的接口
public interface HandleAdapter {
    public boolean support(Object handler);
    public void handle(Object handler);
}
//判断当前的adapter是否支持这个转变，直接返回相应的类
public class HttpAdapter implements HandleAdapter {
    @Override
    public boolean support(Object handler) {
        return (handler instanceof HttpController);
    }

    @Override
    public void handle(Object handler) {
        ((HttpController)handler).doHttpHandler();
    }
}
public class SimpleHandleAdapter implements HandleAdapter {
    @Override
    public boolean support(Object handler) {
        return (handler instanceof SimpleController);
    }

    @Override
    public void handle(Object handler) {
        ((SimpleController)handler).doSimpleHandler();
    }
}
public interface Controller {

}
//具体的实现方式
public class HttpController implements Controller {
    public void doHttpHandler(){
        System.out.println("http....");
    }
}
public class SimpleController implements Controller {
    public void doSimpleHandler(){
        System.out.println("simple....");
    }
}
```
Spring MVC中的Dispatcher根据请求的不同找到不同的适配器。
有一个单独的处理机制,循环判断这个机制是否符合转换机制,如果符合，然后执行处理机制。

## 适配器模式的优点
>* 将目标类和适配者类解耦，通过引入一个适配器类来重用现有的适配者类，而无须修改原有代码。
>* 增加了类的透明性和复用性，将具体的实现封装在适配者类中，对于客户端类来说是透明的，而且提高了适配者的复用性。
>* 灵活性和扩展性都非常好，通过使用配置文件，可以很方便地更换适配器，也可以在不修改原有代码的基础上增加新的适配器类，完全符合“开闭原则”。

类适配器模式还具有如下优点：
由于适配器类是适配者类的子类，因此可以在适配器类中置换一些适配者的方法，使得适配器的灵活性更强。
对象适配器模式还具有如下优点：
一个对象适配器可以把多个不同的适配者适配到同一个目标，也就是说，同一个适配器可以把适配者类和它的子类都适配到目标接口。