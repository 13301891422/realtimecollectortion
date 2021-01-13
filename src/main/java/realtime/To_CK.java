package realtime;

import bean.ZeyiDriver;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import util.MailUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2020/12/09 09:02:20
 */

public class To_CK {
    public static String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static String GROUP_ID = "group.id";
    public static String TOPIC = "topic";
    public static String SQL = "sql";
    public static Logger log = LoggerFactory.getLogger(To_CK.class);

    public static void main(String[] args) {
        //从命令行获取参数
        ParameterTool params = ParameterTool.fromArgs(args);
        //获得环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置程序失败自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 3000l));

        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(2); //设置并发为1，可防止打印控制台乱序

        //从Kafka读取数据
        Properties pros = new Properties();
//        pros.setProperty("bootstrap.servers", "collector101:9092,collector102:9092");
//        pros.setProperty("group.id", "test_zeyiDriver_group");

        pros.setProperty("bootstrap.servers", params.get(BOOTSTRAP_SERVERS));
        pros.setProperty("group.id", params.get(GROUP_ID));

        pros.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer010<String> consumerZeyiDriver = new FlinkKafkaConsumer010<>(params.get(TOPIC), new SimpleStringSchema(), pros);

//        consumerZeyiDriver.setStartFromTimestamp();   //从kafka的何时时间点进行消费

        DataStreamSource<String> sourceDs = env.addSource(consumerZeyiDriver).setParallelism(1);


        SingleOutputStreamOperator<ZeyiDriver> mapDStream = sourceDs.flatMap(
                (FlatMapFunction<String, ZeyiDriver>) (ZeyiDriverArray, out) -> {
                    try {
                        JSONArray zeyiDriverJsonArray = JSONArray.parseArray(ZeyiDriverArray);
                        for (int i = 0; i < zeyiDriverJsonArray.size(); i++) {
                            JSONObject zeyiDriverJson = zeyiDriverJsonArray.getJSONObject(i);
                            ZeyiDriver zeyiDriver = zeyiDriverJson.toJavaObject(ZeyiDriver.class);
                            out.collect(zeyiDriver);
                        }
                    } catch (Exception e) {
                        log.error("源头kafka数据异常,请检查数据格式！！  " + "数据为: " + ZeyiDriverArray ,e);
                    }
                }
        ).name("FlatMapFunction").setParallelism(2);

//        String sql = "insert into ZeyiDriver_test values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        mapDStream.print();

        try {
            mapDStream.addSink(JdbcSink.sink(params.get(SQL), new CkSinkBuilder(), new JdbcExecutionOptions
                                                    .Builder()
                                                    .withBatchSize(300)      //批量写入的条数
                                                    .withBatchIntervalMs(180000L)//批量写入的时间间隔/ms
                                                    .withMaxRetries(5)         //插入重试次数
                                                    .build(),
                                                new JdbcConnectionOptions
                                                    .JdbcConnectionOptionsBuilder()
                                                    .withUrl("jdbc:clickhouse://172.16.182.181:8123/default")
                                                    .withUsername("")
                                                    .withPassword("")
                                                    .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                                    .build()
                                    )
                    ).name("ToClickHouse").setParallelism(4);
            env.execute("输出ClickHouse");
        } catch (Exception e) {
            log.error("数据入库异常！！ 请检查ClickHouse服务是否异常",e);
            MailUtil.sendFailMail("则一速达埋点数据入库异常 请检查ClickHouse服务是否异常！！！");
        }

    }
}