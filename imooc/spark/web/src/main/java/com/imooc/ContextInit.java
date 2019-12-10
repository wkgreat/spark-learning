package com.imooc;

import org.springframework.boot.CommandLineRunner;

public class ContextInit implements CommandLineRunner {

    @Override
    public void run(String... args) throws Exception {
        System.setProperty("hadoop.home.dir","/Users/wkgreat/hadoop/app/hadoop-2.6.0-cdh5.7.0");
    }
}
