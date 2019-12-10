package com.imooc.spark.project.domain

/**
 * 实战课程点击数
 * @param day_course Hbase中的rowkey
 * @param click_count 点击数
 * */
case class  CourseClickCount(day_course:String, click_count:Long)
