# volatile 

缓存行在读取数据的时候，读取64位。

缓存一致性协议:
MESI:modified(修改之后)、exclusive、shared、invalid。


disruptor 使用ringbuffer实现缓存一致性, 前面放7个Long类型变量,INITIAL_CURSOR 变量，正好是64位。



输出汇编码:java -XX:UnlockDiagnosticVMOptions -XX:+PrintAssembly



