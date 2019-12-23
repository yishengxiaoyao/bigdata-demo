# 压缩文件
BufferedInputStream内部封装了一个byte数组用于存放数据，默认指为8192。
FileInputStream中read()方法每次指读取一个字节。
使用缓冲区(BufferedInputStream)，可以提高读取的效率。
需要使用NIO的的知识优化。
## 使用Channel
NIO中推出Channel和ByteBuffer，因为他们的结构更加符合操作系统执行I/O的方式，相对于传统I/O，速度有了显著的提高。
Channel类似于一个包含着煤矿的矿藏，而ByteBuffer则是派送到矿藏的卡车。

NIO中可以使用FileInputStream、FileOutputStream、RandomAccessFile(既能读又能写)产生FileChannel。

```
public static void zipFileChannel() {
    //开始时间
    long beginTime = System.currentTimeMillis();
    File zipFile = new File(ZIP_FILE);
    try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFile));
            WritableByteChannel writableByteChannel = Channels.newChannel(zipOut)) {
        for (int i = 0; i < 10; i++) {
            try (FileChannel fileChannel = new FileInputStream(JPG_FILE).getChannel()) {
                zipOut.putNextEntry(new ZipEntry(i + SUFFIX_FILE));
                fileChannel.transferTo(0, FILE_SIZE, writableByteChannel);
            }
        }
        printInfo(beginTime);
    } catch (Exception e) {
        e.printStackTrace();
    }
}
```
使用transferTo(直接开辟一段直接缓冲区)的方法，将两个通道进行直连。操作系统能够直接传输字节从文件缓存到目标的Channel中，而不需要实际的copy阶段。
copy阶段是从内核空间撞到用户空间的一个过程。

## 直接缓冲区和非直接缓冲区
>* 非直接缓冲区:每次都需要内核在中间作为中转。
>* 直接缓冲区:直接在无力内存申请一块空间，这块空间映射到内核地址空间和用户地址空间，
应用程序与磁盘直接数据的存取通过这块直接申请的物理内存进行交互。

直接缓冲区的缺点:
>* 不安全
>* 消耗更多，因为它不是在JVM中直接开辟空间，这部分内存的回收只能依赖于垃圾回收机制，垃圾什么时候回收不受我们控制。
>* 数据写入物理内存缓冲区中，程序就丧失了对这些数据的管理。

## 内存映射文件
NIO中新出的另一个特性就是内存映射文件，在内存中开辟了一段直接缓冲区。与数据直接作交互。
```
//Version 4 使用Map映射文件
public static void zipFileMap() {
    //开始时间
    long beginTime = System.currentTimeMillis();
    File zipFile = new File(ZIP_FILE);
    try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFile));
            WritableByteChannel writableByteChannel = Channels.newChannel(zipOut)) {
        for (int i = 0; i < 10; i++) {

            zipOut.putNextEntry(new ZipEntry(i + SUFFIX_FILE));

            //内存中的映射文件
            MappedByteBuffer mappedByteBuffer = new RandomAccessFile(JPG_FILE_PATH, "r").getChannel()
                    .map(FileChannel.MapMode.READ_ONLY, 0, FILE_SIZE);

            writableByteChannel.write(mappedByteBuffer);
        }
        printInfo(beginTime);
    } catch (Exception e) {
        e.printStackTrace();
    }
}
```
## 使用Pipe

NIO管道是2个线程之间的单项数据连接。Pipe有一个source(读取数据)通道和一个sink(写入数据)通道。
写入数据会阻塞至有读数据从通道中读取数据。如果没有数据可读，读线程也会阻塞至写线程写入数据，直至通道关闭。
```
//Version 5 使用Pip
public static void zipFilePip() {

    long beginTime = System.currentTimeMillis();
    try(WritableByteChannel out = Channels.newChannel(new FileOutputStream(ZIP_FILE))) {
        Pipe pipe = Pipe.open();
        //异步任务
        CompletableFuture.runAsync(()->runTask(pipe));

        //获取读通道
        ReadableByteChannel readableByteChannel = pipe.source();
        ByteBuffer buffer = ByteBuffer.allocate(((int) FILE_SIZE)*10);
        while (readableByteChannel.read(buffer)>= 0) {
            buffer.flip();
            out.write(buffer);
            buffer.clear();
        }
    }catch (Exception e){
        e.printStackTrace();
    }
    printInfo(beginTime);

}

//异步任务
public static void runTask(Pipe pipe) {

    try(ZipOutputStream zos = new ZipOutputStream(Channels.newOutputStream(pipe.sink()));
            WritableByteChannel out = Channels.newChannel(zos)) {
        System.out.println("Begin");
        for (int i = 0; i < 10; i++) {
            zos.putNextEntry(new ZipEntry(i+SUFFIX_FILE));

            FileChannel jpgChannel = new FileInputStream(new File(JPG_FILE_PATH)).getChannel();

            jpgChannel.transferTo(0, FILE_SIZE, out);

            jpgChannel.close();
        }
    }catch (Exception e){
        e.printStackTrace();
    }
}
```



## 参考文献
[压缩20M文件从30秒到1秒的优化过程](https://mp.weixin.qq.com/s/GXtcb8CNRcNnBdUQDRf5WA)