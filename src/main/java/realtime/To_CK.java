package realtime;

import bean.PayMoney;
import bean.ZeyiDriver;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import util.MailUtil;
import util.ParseJsonData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

class CkSinkBuilder implements JdbcStatementBuilder<ZeyiDriver> {


    @Override
    public void accept(PreparedStatement ps, ZeyiDriver zeyiDriver) throws SQLException {
        if (null != zeyiDriver.getDeviceId()) {ps.setString(1,  zeyiDriver.getDeviceId());} else {ps.setString(1, ""); }
        if (null != zeyiDriver.getDeviceModel()) {ps.setString(2,  zeyiDriver.getDeviceModel());} else {ps.setString(2, ""); }
        if (null != zeyiDriver.getDeviceName()) {ps.setString(3,  zeyiDriver.getDeviceName());} else {ps.setString(3, ""); }
        if (null != zeyiDriver.getOperator()) {ps.setString(4,  zeyiDriver.getOperator());} else {ps.setString(4, ""); }
        if (null != zeyiDriver.getConnectionType()) {ps.setString(5,  zeyiDriver.getConnectionType());} else {ps.setString(5, ""); }
        if (null != zeyiDriver.getSystemType()) {ps.setString(6,  zeyiDriver.getSystemType());} else {ps.setString(6, ""); }
        if (null != zeyiDriver.getSystemVersion()) {ps.setString(7,  zeyiDriver.getSystemVersion());} else {ps.setString(7, ""); }
        if (null != zeyiDriver.getAppName()) {ps.setString(8,  zeyiDriver.getAppName());} else {ps.setString(8, ""); }
        if (null != zeyiDriver.getAppVersion()) {ps.setString(9,  zeyiDriver.getAppVersion());} else {ps.setString(9, ""); }
        if (null != zeyiDriver.getUserId()) {ps.setString(10, zeyiDriver.getUserId());} else {ps.setString(10, ""); }
        if (null != zeyiDriver.getPageName()) {ps.setString(11, zeyiDriver.getPageName());} else {ps.setString(11, ""); }
        if (null != zeyiDriver.getEventType()) {ps.setString(12, zeyiDriver.getEventType());} else {ps.setString(12, ""); }
        if (null != zeyiDriver.getButtonName()) {ps.setString(13, zeyiDriver.getButtonName());} else {ps.setString(13, ""); }
        if (null != zeyiDriver.getCreateTime()) {ps.setString(14, zeyiDriver.getCreateTime());} else {ps.setString(14, ""); }
        if (null != zeyiDriver.getId()) {ps.setString(15, zeyiDriver.getId());} else {ps.setString(15, ""); }
        if (null != zeyiDriver.getBrowser()) {ps.setString(16, zeyiDriver.getBrowser());} else {ps.setString(16, ""); }
        if (null != zeyiDriver.getIpadress()) {ps.setString(17, zeyiDriver.getIpadress());} else {ps.setString(17, ""); }
        if (null != zeyiDriver.getGPSadress()) {ps.setString(18, zeyiDriver.getGPSadress());} else {ps.setString(18, ""); }
        if (null != zeyiDriver.getDingding_user_code()) {ps.setString(19, zeyiDriver.getDingding_user_code());} else {ps.setString(19, ""); }
    }
}

public class To_CK {
    public static String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static String GROUP_ID = "group.id";
    public static String TOPIC = "topic";
    public static String SQL = "sql";
    public static String DATABASE = "database";
    public static final Logger log = LoggerFactory.getLogger(To_CK.class);

    public static void main(String[] args) {
        //从命令行获取参数
        ParameterTool params = ParameterTool.fromArgs(args);
        //获得环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置程序失败自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 3000l));
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(2); //设置并发为1，防止打印控制台乱序
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //Flink 默认使用 ProcessingTime 处理,设置成event time
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);//Table Env 环境
        //从Kafka读取数据
        Properties pros = new Properties();
//        pros.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
//        pros.setProperty(GROUP_ID, params.get(GROUP_ID));
        pros.setProperty("bootstrap.servers", "192.168.20.27:9092");
//          pros.setProperty("bootstrap.servers", "hadoop105:9092");

        pros.setProperty("group.id", "test");
        pros.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer010<String> consumerZeyiDriver = new FlinkKafkaConsumer010<>
                (
                        "zeyidriver",
//                        params.get(TOPIC),
                        new SimpleStringSchema(),
                        pros);

//        consumerZeyiDriver.setStartFromTimestamp();   //从kafka的何时时间点进行消费

        DataStreamSource<String> sourceDs = env.addSource(consumerZeyiDriver);
        String sql = "insert into ZeyiDriver values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        SingleOutputStreamOperator<ZeyiDriver> mapDStream = sourceDs.flatMap(
                new FlatMapFunction<String, ZeyiDriver>() {
                    @Override
                    public void flatMap(String ZeyiDriverArray, Collector<ZeyiDriver> out) throws Exception {
                        try {
                            JSONArray zeyiDriverJsonArray = JSONArray.parseArray(ZeyiDriverArray);
                            for (int i = 0; i < zeyiDriverJsonArray.size(); i++) {
                                JSONObject zeyiDriverJson = zeyiDriverJsonArray.getJSONObject(i);
                                ZeyiDriver zeyiDriver = zeyiDriverJson.toJavaObject(ZeyiDriver.class);
                                out.collect(zeyiDriver);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            log.error("源头kafka数据异常,请检查数据格式！！  " + "数据为: " + ZeyiDriverArray );
                        }
                    }
                }
        ).name("FlatMapFunction");


//        SingleOutputStreamOperator<PayMoney> mapDStream = sourceDs.map(
//                new MapFunction<String, PayMoney>() {
//                    @Override
//                    public PayMoney map(String value) throws Exception {
//                        JSONObject payMoneyJson = ParseJsonData.getJsonData(value);
////                            PayMoney payMoney = new PayMoney();
////                            try {
////                                payMoney.setUid(payMoneyJson.getString("uid"));
////                                payMoney.setPaymoney(payMoneyJson.getString("paymoney"));
////                                payMoney.setVip_id(payMoneyJson.getString("vip_id"));
////                                payMoney.setUpdatetime(payMoneyJson.getString("updatetime"));
////                                payMoney.setSiteid(payMoneyJson.getString("siteid"));
////                                payMoney.setDt(payMoneyJson.getString("dt"));
////                                payMoney.setDn(payMoneyJson.getString("dn"));
////                                payMoney.setCreatetime(payMoneyJson.getString("createtime"));
////                                System.out.println(payMoney.toString());
////                            } catch (Exception e) {
////                                log.error("kafka输入数据异常");
////                            }
//                        PayMoney payMoney = payMoneyJson.toJavaObject(PayMoney.class);
//                        System.out.println(payMoney.toString());
//                        return payMoney;
//                    }
//                }
//        ).setParallelism(4).name("Transform JavaBean");


        try {
            mapDStream
                    .addSink(JdbcSink
                    .sink(
                    //                              params.get(SQL),
                    sql,
                    new CkSinkBuilder(), new JdbcExecutionOptions
                    .Builder()
                    .withBatchSize(5)      //批量写入的条数
//                   .withBatchIntervalMs(10000L)//批量写入的时间间隔/ms
                    .withMaxRetries(1)         //插入重试次数
                    .build(),
                                                    new JdbcConnectionOptions
                                                    .JdbcConnectionOptionsBuilder()
//                                                    .withUrl("jdbc:clickhouse://hadoop105:8123/default")
//                                                    .withUrl("jdbc:clickhouse://47.111.10.168:8123/"+params.get(DATABASE))
//                                                    .withUrl("jdbc:clickhouse://101.37.247.143:8123/default")
                                                    .withUrl("jdbc:clickhouse://47.111.10.168:8123/default")
                                                    .withUsername("")
                                                    .withPassword("")
                                                    .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                                    .build()
                                    )
                    ).name("ToClickHouse");
            env.execute("输出ClickHouse");
        } catch (Exception e) {
            log.error("数据入库异常！！ { }请检查ClickHouse服务是否异常");
            MailUtil.sendFailMail("则一速达埋点数据入库异常 请检查ClickHouse服务是否异常！！！");
        }
    }
}
