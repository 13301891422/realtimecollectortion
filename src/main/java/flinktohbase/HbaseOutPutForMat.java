package flinktohbase;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2021/1/5 9:09:25
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import flinktohbase.hbase.HbaseConnectionPool;
import flinktohbase.tool.ConnectionPoolConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;

class HbaseOutPutForMat extends RichSinkFunction<String> {
    private static ConnectionPoolConfig config;
    private static Connection connection;
    private static HbaseConnectionPool pool;
    private static Admin admin;
    private static String columnFamily = "info";
    private static SimpleDateFormat df;
    private static Table HbaseTable;
    private static TableName tableName;
    private static String nameSpace;
    private static String profix;
    private static String suffix;
    private static JSONObject jsonObject;
    private static String table1;
    private static JSONArray dataArray;
    private static JSONObject jsonData;
//    private static String collector_time = null;
    public HbaseOutPutForMat(String nameSpace, String profix, String suffix) {
        this.nameSpace = nameSpace;
        this.profix = profix;
        this.suffix = suffix;
    }

    public static String getNameSpace() {
        return nameSpace;
    }

    public static void setNameSpace(String nameSpace) {
        HbaseOutPutForMat.nameSpace = nameSpace;
    }

    public static String getProfix() {
        return profix;
    }

    public static void setProfix(String profix) {
        HbaseOutPutForMat.profix = profix;
    }

    public static String getSuffix() {
        return suffix;
    }

    public static void setSuffix(String suffix) {
        HbaseOutPutForMat.suffix = suffix;
    }

    @Override
    //初始化连接池 获取Hbase连接 该方法只在程序第一次启动时调用一次
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        config = new ConnectionPoolConfig();
        //设置Hbase最大连接数
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);
        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig = HBaseConfiguration.create();
        //设置Hbase依赖的zookeeper地址
        hbaseConfig.set("hbase.zookeeper.quorum", "hadoop101:2181,hadoop102:2181,hadoop103:2181");
        hbaseConfig.set("hbase.defaults.for.version.skip", "true");
        pool = new HbaseConnectionPool(config, hbaseConfig);
        connection = pool.getConnection();
        admin = connection.getAdmin();
    }

    @Override
    //每来一条数据便会调用该方法
    public void invoke(String value, Context context) throws Exception {

        //将String转为JSON
        jsonObject = JSONObject.parseObject(value);
        //获取JSON中的表名
        table1 = jsonObject.getString("table");

        //获取时间戳
        String collector_time_timeStamp = jsonObject.getString("es");

        //生成Hbase的表名
        tableName = TableName.valueOf(HbaseOutPutForMat.getNameSpace() + ":" + HbaseOutPutForMat.getProfix() + table1 + HbaseOutPutForMat.getSuffix()
        );

        //判断即将写入数据的Hbase表是否存在 不存在就新建
        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily)));
        }

        //获取即将写入的Hbase表
        HbaseTable = connection.getTable(tableName);

//        //设置Hbase中的数据的RowKey为时间+8位随机数

        //获取JSON中的data 获取的data是一个JSON数组
        // [{"no":"aaa","school":"2200","class":"222","address":"333","name":"xxx","teacher":"oo","age":"30","shijian":"null"}
        // ,{"no":"xgh","school":"432","class":"3423","address":"67","name":"2","teacher":"2","age":"4","shijian":"ll"}]

        //获取数据数组 原本为String类型
        String data = jsonObject.getString("data");
        //将数据数组转换成JsonArray
        dataArray = JSONArray.parseArray(data);

        //遍历JSON数组 取出每一条JSON数据
        for (Object o : dataArray.toArray()) {
            //设置时间格式 精确到时分秒
            df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //Hbase的主键设置 为随机8位Int类型的数值 + 当前的时间
            Put put = new Put(Bytes.toBytes(new Random().nextInt(100000000) + "_" + df.format(new Date())));

            //  aa的数据格式为:
            // {"no":"aaa","school":"2200","class":"222","address":"333","name":"xxx","teacher":"oo","age":"30","shijian":"null"}
            jsonData = JSONObject.parseObject(JSON.toJSONString(o));
            //    JSONObject aa = (JSONObject) o;
            //遍历JSON 取出key和Value 并写入Hbase
            for (Map.Entry<String, Object> entry : jsonData.entrySet()) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue().toString())
                );
            }

            String collector_time = df.format(new Date(Long.valueOf(collector_time_timeStamp)));
            //新增collector 采集数据
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("collector_time"), Bytes.toBytes(collector_time));
            //写入Hbase表
            HbaseTable.put(put);
        }
            //关闭HbaseTable
            HbaseTable.close();
    }


    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
