package flinktohbase;

import java.util.Properties;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.MailUtil;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2021/1/5 9:05:37
 */
public class Flink2Hbase {
    private static JSONObject jsonObject = null;
    private static String type = null;

    public static void main(String[] args) {
        //设置Flink运行时环境(流)
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.enableCheckpointing(120000l, CheckpointingMode.AT_LEAST_ONCE); // 非常关键，一定要设置启动检查点！!
        //设置程序失败自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 2000L));

        //设置并行度(线程数)
        env.setParallelism(2);

        //kafka的参数设置
        Properties props = new Properties();
//        props.setProperty("zookeeper.connect", ZOOKEEPER_HOST);
        props.setProperty("bootstrap.servers", "collector101:9092,collector102:9092");
        props.setProperty("group.id", "test-consumer-group");

        String kafkaTopicName = args[0];
        String NameSpace = args[1];
        String prefix = args[2];
        String suffix = args[3];

        System.out.println(kafkaTopicName);
        System.out.println(NameSpace);
        System.out.println(prefix);
        System.out.println(suffix);

        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>
                (kafkaTopicName, new SimpleStringSchema(), props);

//        consumer011.setStartFromTimestamp(1609743600000L);

        DataStream<String> kafkaDStream = env.addSource(consumer011).name("KafkaSource").setParallelism(2);

//        DataStreamSource<String> kafkaDStream = env.socketTextStream("localhost", 9999);

        OutputTag<String> deleteStream = new OutputTag<String>("deleteStream"){};
        OutputTag<String> InsertOrUpdateStream = new OutputTag<String>("InsertOrUpdateStream"){};
        OutputTag<String> AlterStream = new OutputTag<String>("AlterStream"){};

        SingleOutputStreamOperator<String> process = kafkaDStream.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        jsonObject = JSONObject.parseObject(value);
                        type = jsonObject.getString("type");
                        switch (type) {
                            case "INSERT": ctx.output(InsertOrUpdateStream, value);break;
                            case "UPDATE": ctx.output(InsertOrUpdateStream, value);break;
                            case "DELETE": ctx.output(deleteStream, value);break;
                            case "ALTER": ctx.output(AlterStream, value);break;
                            case "CREATE": ctx.output(AlterStream, value);break;
                            case "QUERY": ctx.output(AlterStream, value);break;
                            default: break;
                        }
                        ;
                    }
                }
        ).setParallelism(4);

        process.getSideOutput(deleteStream).addSink(new MysqlSink()).name("deleteSink");
        process.getSideOutput(InsertOrUpdateStream).addSink(new HbaseOutPutForMat(NameSpace, prefix, suffix)).setParallelism(3).name("HbaseSink");
        process.getSideOutput(AlterStream).addSink(new MysqlSink()).name("AlterSink");

//        SingleOutputStreamOperator<String> updateDStream = kafkaDStream.filter((FilterFunction<String>) value -> {
//            jsonObject = JSONObject.parseObject(value);
//            type = jsonObject.getString("type");
//            return Objects.equals(type, "ALTER") || Objects.equals(type, "CREATE") || Objects.equals(type, "QUERY");
//        });
//
//
//        updateDStream.addSink(new MysqlSink());
//
//        SingleOutputStreamOperator<String> filter = kafkaDStream.filter((FilterFunction<String>) value -> {
//            jsonObject = JSONObject.parseObject(value);
//            type = jsonObject.getString("type");
//            table = jsonObject.getString("table");
//            return Objects.equals(type, "INSERT") || Objects.equals(type, "UPDATE");
//        });
//
//        //将kafka流中的数据经过map转换后写入Hbase
//        filter.addSink(new HbaseOutPutForMat(NameSpace, prefix, suffix));
        try {
            env.execute("tms_job");
        } catch (Exception e) {
            e.printStackTrace();
            MailUtil.sendFailMail("Canal-Kafka-Hbase实时采集任务失败  请检查数据量是否过大！！！");
        }
    }

}
