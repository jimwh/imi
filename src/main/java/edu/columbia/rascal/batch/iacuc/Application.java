package edu.columbia.rascal.batch.iacuc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

@Configuration
@EnableAutoConfiguration
@ImportResource( { "application-context.xml" } )
@ComponentScan("edu.columbia.rascal")
public class Application {

    private static final Logger log= LoggerFactory.getLogger(Application.class);
    public static void main(String[] args) {
        ApplicationContext ctx= SpringApplication.run(Application.class, args);
        Foo foo = ctx.getBean(Foo.class);
        // foo.test();
        // foo.startup();
        // foo.testGetNote();
        // foo.testSubset();
        // foo.testTables();
        //
        // foo.printHistoryByBizKey(10633);
        //

        //
        // foo.testAdverse();
        // foo.testKaputAdverse();
        // foo.testInProgressAdverse();

        foo.startup();
        foo.shutdown();
        SpringApplication.exit(ctx);
        log.info("application done...");

    }

    static void foobar() {
        List<String> list=new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");
        list.add("d");
        list.add("e");
        log.info(list.toString());
        Deque<String>deque=new LinkedList<String>();
        for(int index=list.size()-1; index>-1; index--) {
            deque.addFirst(list.get(index));
        }
        log.info("first={}", deque.getFirst());
        String first = deque.removeFirst();
        log.info("first={}", first);
        for(String str: deque) {
            log.info("str={}", str);
        }

        // change deque to list
        List<String>fooList=new ArrayList<String>();
        for(String str: deque) {
            fooList.add(str);
        }
        log.info(fooList.toString());
    }
}
