package com.imooc.spark;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

@RestController
public class HelloBoot {

    @GetMapping("/hello")
    public String sayHello() {
        return "Hello world Sring boot";
    }

    @GetMapping("/first")
    public ModelAndView firstDemo() {
        return new ModelAndView("test");
    }

    @GetMapping("/course_clickcount")
    public ModelAndView courseClickCountStat() {
        return new ModelAndView("demo");
    }

}
