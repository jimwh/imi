package edu.columbia.rascal.batch.iacuc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import java.util.*;

@Configuration
@EnableAutoConfiguration
@ImportResource( { "application-context.xml" } )
@ComponentScan("edu.columbia.rascal")
public class Application {

    private static final Logger log= LoggerFactory.getLogger(Application.class);
    public static void main(String[] args) {
        List<String> list = new ArrayList<String>();
        list.add("foo");
        list.add("me");
        list.add("for");
        list.add("www");

        Deque<String> linkedList=new LinkedList<String>();
        for(int i=list.size()-1; i>-1; i--) {
            log.info("addFirst=" + list.get(i));
            linkedList.addFirst(list.get(i));
        }

        for(String str: linkedList) {
            log.info(str);
        }

        ApplicationContext ctx= SpringApplication.run(Application.class, args);
        Foo foo = ctx.getBean(Foo.class);
        foo.test();
        //foo.startup();
        SpringApplication.exit(ctx);
    }
}
