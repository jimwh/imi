package edu.columbia.rascal.batch.iacuc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

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
        foo.printHistoryByBizKey(10633);
        // foo.startup();
        SpringApplication.exit(ctx);
        log.info("application done...");
    }
}
