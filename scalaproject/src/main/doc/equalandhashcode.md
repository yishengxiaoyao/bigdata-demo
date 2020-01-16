# equal vs hashcode vs ==
## ==
 == 运算符是用来比较两个变量的值是否相等。该运算符用于比较变量对应用的内存中锁存储的数值是否相同,
要比较两个基本类型的数据或者两个引用(两个变量是否指向同一个对象)是否相等,只能使用 == 运算符。
## equals
equals 是Object类的一个方法。
Object类中定义的equals(Object)方法是直接使用 == 运算符比较的两个对象,索引没有覆盖equals(Object)方法的情况下,
equals(Object)与 == 运算符一样,比较的是引用。

相对于 == 运算符,equals(Object)方法的特殊指出就在于它可以被覆盖,可以通过覆盖的方法让它不是比较引用而是比较内容。

## hashcode
hashCode()方法是从Object类中继承过来的,它也用来坚定两个对象是否相等。Object类中的hashCode()方法返回对象在内存中地址转换
成一个int值,如果没有重新hashCode()方法，任何对象的hashCode()方法都是不相等的。

equals是用给户调用的,如果需要判断两个对象是否相等,可以重新equals(Object)方法,然后在代码中调用,这样就可以判断他们是否相等。
hashCode()方法，用户一般不会调用。

equals相等 --> hashCode 一定相等
equals不等 --> hashCode 可能相等,也可能不想等
hashCode相等 --> equals不一定相等
hashCode不相等 --> equals 一定不想等