# 工厂模式
## 简单工厂模式
在简单工厂模式中，可以根据参数的不同返回不同类的实例。简单工厂模式专门定义一个类来负责创建其他类的实例，被创建的实例通常都具有共同的父类。

就是根据参数，来找到相应的对象类

```
public class PizzaFactory {
    //返回的可以类可以继承抽象类或者接口
    //本例中没有定义多个对象，现实中可以根据条件返回不同的对象
    public static Pizza createPizza(String orderType){
        if (orderType.equalsIgnoreCase("")){
            return new ChessPizza();
        }
        return null;
    }
}
```
### 简单工厂模式优点
>* 工厂根据参数不同,创建不同的对象,实现了责任划分。
>* 减少消费者的记忆
>* 引入配置文件，不用修改客户端的方法

### 简单工厂模式缺点
>* 工厂类集中创建所有的产品,一旦不同正常工作,整个系统受影响
>* 系统扩展性差,如果创建新的产品,增加新的产品和判断逻辑。

### 简单工厂使用场景
>* 工厂类负责创建的对象比较少：由于创建的对象较少，不会造成工厂方法中的业务逻辑太过复杂。
>* 客户端只知道传入工厂类的参数，对于如何创建对象不关心：客户端既不需要关心创建细节，甚至连类名都不需要记住，只需要知道类型所对应的参数。

JDK中的时间格式化类就是简单工厂模式

去味多美吃披萨,如果给店员说要什么味的披萨,店员就可以给什么披萨,披萨的种类较少,业务逻辑没有那么复杂。

## 工厂模式
在工厂方法模式中，工厂父类负责定义创建产品对象的公共接口，而工厂子类则负责生成具体的产品对象。

一个工厂只能创建一种产品。即将工厂和产品进行抽象，每个工厂只能生成一种产品。在扩展的时候，可以直接添加新的类，实现相应的方法，就可以啦
```
public interface CarFactory {
    Car makeCar();
}
public interface Car {
    public void make();
}
```
```
public class DaZhong implements Car {
    @Override
    public void make() {

    }
}
public class DaZhongFactory implements CarFactory{
    @Override
    public Car makeCar() {
        return new DaZhong();
    }
}
```

在早期的时候，每个汽车制造商，只能生成一种汽车，要生产新的车型只能建新的生产线和新的产品模具。

工厂模式应用于JDBC源代码中。


###工厂模式的优点 
>* 在工厂方法模式中，工厂方法用来创建客户所需要的产品，同时还向客户隐藏了哪种具体产品类将被实例化这一细节，用户只需要关心所需产品对应的工厂，无须关心创建细节，甚至无须知道具体产品类的类名。
>* 基于工厂角色和产品角色的多态性设计是工厂方法模式的关键。它能够使工厂可以自主确定创建何种产品对象，而如何创建这个对象的细节则完全封装在具体工厂内部。工厂方法模式之所以又被称为多态工厂模式，是因为所有的具体工厂类都具有同一抽象父类。
>* 使用工厂方法模式的另一个优点是在系统中加入新产品时，无须修改抽象工厂和抽象产品提供的接口，无须修改客户端，也无须修改其他的具体工厂和具体产品，而只要添加一个具体工厂和具体产品就可以了。这样，系统的可扩展性也就变得非常好，完全符合“开闭原则”。

### 工厂模式的缺点
>* 在添加新产品时，需要编写新的具体产品类，而且还要提供与之对应的具体工厂类，系统中类的个数将成对增加，在一定程度上增加了系统的复杂度，有更多的类需要编译和运行，会给系统带来一些额外的开销。
>* 由于考虑到系统的可扩展性，需要引入抽象层，在客户端代码中均使用抽象层进行定义，增加了系统的抽象性和理解难度，且在实现时可能需要用到DOM、反射等技术，增加了系统的实现难度。


### 抽象工厂模式

抽象工厂模式(Abstract Factory Pattern)：提供一个创建一系列相关或相互依赖对象的接口，而无须指定它们具体的类。

一个汽车生产商可以生产多种车型,一家汽车生产商(一个大型企业可以在各地都有工厂)可以满足一个汽车公司的所有产品(多种产品)需求。

```
public abstract class ProductFactory {
    public abstract ProductA createProductA();
    public abstract ProductB createProductB();
}
public class ProductFactoryA extends ProductFactory {
    @Override
    public ProductA createProductA() {
        return new ProductA1();
    }

    @Override
    public ProductB createProductB() {
        return new ProductB1();
    }
}
public class ProductFactoryB extends ProductFactory {
    @Override
    public ProductA createProductA() {
        return new ProductA1();
    }

    @Override
    public ProductB createProductB() {
        return new ProductB1();
    }
}
```
```
public abstract class ProductA {
    public abstract void use();
}
public class ProductA1 extends ProductA {
    @Override
    public void use() {
        System.out.println("==producta1 use===");
    }
}
public abstract class ProductB {
    public abstract void eat();
}
public class ProductB1 extends ProductB {
    @Override
    public void eat() {
        System.out.println("==productb1 use===");
    }
}
```
一家公司可以有多个产品,例如华为和小米，都可以生产手机和电脑。

### 抽象工厂优点
>* 抽象工厂模式隔离了具体类的生成，使得客户并不需要知道什么被创建。由于这种隔离，更换一个具体工厂就变得相对容易。所有的具体工厂都实现了抽象工厂中定义的那些公共接口，因此只需改变具体工厂的实例，就可以在某种程度上改变整个软件系统的行为。另外，应用抽象工厂模式可以实现高内聚低耦合的设计目的，因此抽象工厂模式得到了广泛的应用。
>* 当一个产品族中的多个对象被设计成一起工作时，它能够保证客户端始终只使用同一个产品族中的对象。这对一些需要根据当前环境来决定其行为的软件系统来说，是一种非常实用的设计模式。
>* 增加新的具体工厂和产品族很方便，无须修改已有系统，符合“开闭原则”。
###  抽象工厂缺点
>* 在添加新的产品对象时，难以扩展抽象工厂来生产新种类的产品，这是因为在抽象工厂角色中规定了所有可能被创建的产品集合，要支持新种类的产品就意味着要对该接口进行扩展，而这将涉及到对抽象工厂角色及其所有子类的修改，显然会带来较大的不便。
>* 开闭原则的倾斜性（增加新的工厂和产品族容易，增加新的产品等级结构麻烦）。
###  抽象工厂适用环境
    
>* 一个系统不应当依赖于产品类实例如何被创建、组合和表达的细节，这对于所有类型的工厂模式都是重要的。
>* 系统中有多于一个的产品族，而每次只使用其中某一产品族。
>* 属于同一个产品族的产品将在一起使用，这一约束必须在系统的设计中体现出来。
>* 系统提供一个产品类的库，所有的产品以同样的接口出现，从而使客户端不依赖于具体实现。

