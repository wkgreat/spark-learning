package com.imooc.spark;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.imooc.dao.CourseClickCountDAO;
import com.imooc.domain.CourseClickCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class ImoocStatApp {

    private static Map<String, String> courses = new HashMap<>();

    static {
        courses.put("112","Spark SQL慕课网日志分析");
        courses.put("128","10小时入门大数据");
        courses.put("145","深度学习之神经网络核心原理与算法");
        courses.put("146","强大的Node.js在Web开发的应用");
        courses.put("131","Vue+Django实战");
        courses.put("130","Web前端性能的优化");
    }

    @Autowired
    CourseClickCountDAO courseClickCountDAO;

    Gson gson = new Gson();

    @PostMapping("/course_clickcount_dynamic")
    public List<CourseClickCount> courseClickCount() throws Exception{

        List<CourseClickCount> list = courseClickCountDAO.query("20191028");
        list.forEach(r->r.setName(courses.get(r.getName().substring(9))));
        return list;

    }

    @GetMapping("/echarts")
    public ModelAndView echarts() {
        return new ModelAndView("echarts");
    }



}
