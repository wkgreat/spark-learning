package com.imooc.dao;

import com.imooc.domain.CourseClickCount;
import com.imooc.utils.HBaseUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class CourseClickCountDAO {

    public List<CourseClickCount> query(String day) throws Exception {

        Map<String, Long> map = HBaseUtils.getInstance().query("imooc_course_clickcount",day);
        return map
                .entrySet()
                .stream()
                .map(r->new CourseClickCount(r.getKey(),r.getValue()))
                .collect(Collectors.toList());

    }

}
