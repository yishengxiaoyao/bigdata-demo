# 建造者模式
造者模式(Builder Pattern)：将一个复杂对象的构建与它的表示分离，使得同样的构建过程可以创建不同的表示。

建造者模式是一步一步创建一个复杂的对象，它允许用户只通过指定复杂对象的类型和内容就可以构建它们，用户不需要知道内部的具体构建细节。建造者模式属于对象创建型模式。

建造者模式就是将一系列属性拼装到一个对象中。

类似于盖房子,需要有工程师，现将房子的大致结构画出来,后然后工人按照图纸建造房子。



```
//工程制图(这是按照用户的需求)
public class HouseDirector {
    //工程监理
    private HouseBuilder houseBuilder;

    public HouseDirector(HouseBuilder houseBuilder) {
        this.houseBuilder = houseBuilder;
    }
    //工程监理按照图纸建造房子
    public House buildHouse(){
        houseBuilder.buildBase();
        houseBuilder.buildWall();
        return houseBuilder.buildHouse();
    }
}
```
```
//工程监理公司有好多监理，都可以看到看图纸，建造房子
public interface HouseBuilder {
    public void buildBase();
    public void buildWall();
    public House buildHouse();
}
//监理在收到图纸之后，知道要建什么的房子应该如何建造，在建成之后,会有验收
public class ConcreteHouseBuilder implements HouseBuilder {
    //组合
    private House house = new House();

    @Override
    public void buildBase() {
        house.setBase("build base");
    }

    @Override
    public void buildWall() {
        house.setBase("build wall");
    }

    @Override
    public House buildHouse() {
        return house;
    }
}
```

### 建造者的优点
>* 客户端不必知道产品内部组成的细节，将产品本身与产品的创建过程解耦，使得相同的创建过程可以创建不同的产品对象。
>* 用户使用不同的具体建造者即可得到不同的产品对象。
>* 可以更加精细地控制产品的创建过程 
>* 增加新的具体建造者无须修改原有类库的代码，指挥者类针对抽象建造者类编程，系统扩展方便，符合“开闭原则”。

### 建造者缺点
>* 建造者模式所创建的产品一般具有较多的共同点，其组成部分相似，如果产品之间的差异性很大，则不适合使用建造者模式，因此其使用范围受到一定的限制。
>* 如果产品的内部变化复杂，可能会导致需要定义很多具体建造者类来实现这种变化，导致系统变得很庞大。

建造者模式和工厂模式的区别:
>* 建造者模式返回一个组装好的完整产品,而抽象工厂模式返回一系列相关的产品，这些产品位于不同的产品等级结构，构成了一个产品族。 
>* 在抽象工厂模式中，客户端实例化工厂类，然后调用工厂方法获取所需产品对象，而在建造者模式中，客户端可以不直接调用建造者的相关方法，而是通过指挥者类来指导如何生成对象，包括对象的组装过程和建造步骤，它侧重于一步步构造一个复杂对象，返回一个完整的对象。\
>* 抽象工厂类需要关注细节,建造者模式不需要关心细节,只告诉需要什么。