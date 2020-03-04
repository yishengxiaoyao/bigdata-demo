# 单例模式
单例模式(Singleton Pattern)：单例模式确保某一个类只有一个实例，而且自行实例化并向整个系统提供这个实例，这个类称为单例类，它提供全局访问的方法。
单例模式的要点有三个：一是某个类只能有一个实例；二是它必须自行创建这个实例；三是它必须自行向整个系统提供这个实例。单例模式是一种对象创建型模式。单例模式又名单件模式或单态模式。

在单例模式的实现过程中，需要注意如下三点：
>* 单例类的构造函数为私有；
>* 提供一个自身的静态私有成员变量；
>* 提供一个公有的静态工厂方法。

## 懒汉式单例
```
public class LazySingleton {
    private LazySingleton(){

    }
    private static LazySingleton INSTANCE = new LazySingleton();

    public static LazySingleton getInstance(){
        return INSTANCE;
    }
}
```
## 饿汉式单例
```
public class HungrySingleton {
    private HungrySingleton(){
    }
    private static HungrySingleton INSTANCE ;

    public static HungrySingleton getInstance(){
        if (INSTANCE == null){
            return new HungrySingleton();
        }
        return INSTANCE;
    }
}
```
##内部类单例
```
public class InnerSingleton {
    private InnerSingleton(){

    }
    private static class InnerClass{
        private static final InnerSingleton INSTANCE = new InnerSingleton();
    }
    public static InnerSingleton getInstance(){
        return InnerClass.INSTANCE;
    }
}
```
## 双重检查
```
//必须要进行双重检查因为创建对象的中间态，会获取，会造成影响，需要将实例变成volatile
public class DoubleCheckSingleton {
    private DoubleCheckSingleton(){

    }
    private volatile static DoubleCheckSingleton INSTANCE;
    public static DoubleCheckSingleton getInstance(){
        while (INSTANCE == null){
            synchronized (DoubleCheckSingleton.class){
                if (INSTANCE == null){
                    INSTANCE = new DoubleCheckSingleton();
                }
            }
        }
        return INSTANCE;
    }
}
``` 

##枚举单例
```
//这种是安全的代理，因为JVM碰到这种单例进行反射时，会出现异常。
public enum EnumSingleton {
    INSTANCE;
    public EnumSingleton getInstance(){
        return INSTANCE;
    }
}
```