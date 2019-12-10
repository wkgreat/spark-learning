package com.imooc.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * HBASE工具类，单例模式
 * */
public class HBaseUtils {

    HBaseAdmin admin = null;
    Configuration configuration = null;

    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","hadoop000:2181");
        configuration.set("hbase.rootdir","hdfs://hadoop000:8020/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;
    public static synchronized HBaseUtils getInstance() {
        if(null==instance) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public void put(String tableName, String rowkey, String cf, String column, String value) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据表名和输入条件获取Hbase的记录数
     * */
    public Map<String, Long> query(String tableName, String condition) throws IOException {

        Map<String, Long> map = new HashMap<>();
        HTable table = getTable(tableName);
        String cf = "info";
        String qualifer = "click_count";
        Scan scan = new Scan();
        Filter filter = new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs) {
            String row = Bytes.toString(result.getRow());
            long clickcount = Bytes.toLong(result.getValue(cf.getBytes(),qualifer.getBytes()));
            map.put(row, clickcount);
        }
        return map;
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","/Users/wkgreat/hadoop/app/hadoop-2.6.0-cdh5.7.0");
        Map<String, Long> map = new HashMap<>();
        try {
            map = HBaseUtils.getInstance().query("imooc_course_clickcount","20191028");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(Map.Entry<String,Long> entry : map.entrySet()) {
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }

}
