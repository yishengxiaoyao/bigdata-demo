# Spring Boot 与 Thymeleaf 结合生成 PDF
本文使用的Spring Boot版本为2.1.4.RELEASE
## 导入依赖
```pom
<dependency>
    <groupId>org.xhtmlrenderer</groupId>
    <artifactId>flying-saucer-pdf</artifactId>
    <version>9.1.18</version>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-thymeleaf</artifactId>
</dependency>    
```
## 服务类
服务接口:
```java
package com.edu.pdf.service;
public interface PdfService {
    void generatePdf();
}
```
服务实现类:
```java
package com.edu.pdf.service.impl;

import com.lowagie.text.DocumentException;

import com.edu.pdf.service.IMailService;
import com.edu.pdf.service.PdfService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.xhtmlrenderer.pdf.ITextRenderer;

import javax.mail.MessagingException;
import java.io.Map;
import java.util.HashMap;
import java.util.Map;

@Service
public class LucrativeServiceImpl implements LucrativeService {
 
    @Autowired
    private IMailService mailService;

    @Autowired
    private TemplateEngine templateEngine;
    
    @Override
    public void generatePdf() {
        Context context = new Context();
        Map<String,Object> variables = new HashMap<>();
        variables.put("name","yishengxiaoyao");
        variables.put("days",30);
        context.setVariables(variables);
        String content = templateEngine.process("email",context);
        FileOutputStream out = null;
        String fileName = "20191113";
        try {
            final File outputFile = File.createTempFile(fileName,".pdf") ;
            out = new FileOutputStream(outputFile);
            ITextRenderer renderer = new ITextRenderer();
            renderer.setDocumentFromString(content);
            renderer.layout();
            renderer.createPDF(out,false);
            renderer.finishPDF();
            System.out.println("==pdf created successfully==");
            System.out.println(outputFile.getAbsolutePath());
            mailService.sendAttachmentsMail("644875343@qq.com","test","This is a test",outputFile.getPath());
        }catch (IOException e){

        }catch (DocumentException e){

        }catch (MessagingException e){
            System.out.println("====send messsage failure!====");
        }finally {
            if (out!=null){
                try {
                    out.close();
                }catch (IOException e){

                }
            }
        }
    }
}
```
由于代码中使用了邮件发送，需要参考[spring-boot-email-demo](https://github.com/yishengxiaoyao/spring-boot-email-demo)。

## 参考文献
[How To Create PDF through HTML Template In Spring Boot](https://www.oodlestechnologies.com/blogs/How-To-Create-PDF-through-HTML-Template-In-Spring-Boot/)
[springboot通过thymeleaf模板实现动态html模板转pdf文件](https://blog.csdn.net/victory_chao/article/details/88861479)
[Flying_Saucer_PDF_Generation](https://github.com/tuhrig/Flying_Saucer_PDF_Generation)
[iTextRenderer(Flying Saucer) HTML转PDF](https://www.cnblogs.com/reese-blogs/p/5546806.html)