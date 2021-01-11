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
    private static JSONObject jsonObject ;
    private static String type ;

    public static void main(String[] args) {
        //设置Flink运行时环境(流)

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置程序失败自动重启策略 程序宕机后,自动重启5次 重启间隔为2s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 2000L));

        //设置并行度(线程数) 为什么设置为1？ 因为kafka的分区数我们这边只用了一个
        env.setParallelism(1);

        //kafka的参数设置
        Properties props = new Properties();
        //kafka集群的地址
        props.setProperty("bootstrap.servers", "collector101:9092,collector102:9092");
        //设置kafka的消费者组
        props.setProperty("group.id", "test-consumer-group");

        //kafka的topic名称
        String kafkaTopicName = args[0];
        //Hbase的数据库名
        String NameSpace = args[1];
        //Hbase的表名前缀
        String prefix = args[2];
        //Hbase的表名后缀
        String suffix = args[3];

        //新增kafka消费者 用来消费kafka集群的消息 消息类型为String类型
        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(kafkaTopicName, new SimpleStringSchema(), props);

        //设置从kafka的指定时间点开始消费 如消费昨天3点往后的数据
//        consumer011.setStartFromTimestamp(1609743600000L);

        //加kafka Source加入Flink运行的流环境中 设置单个Source的并行度(线程数)为1
        DataStream<String> kafkaDStream = env.addSource(consumer011).name("KafkaSource").setParallelism(1);

        //侧输出流
        //针对tms或vehicle库中的删除类型的数据
        OutputTag<String> deleteStream = new OutputTag<String>("deleteStream"){};
        //针对tms或者vehicle库中新增或者修改类型的数据
        OutputTag<String> InsertOrUpdateStream = new OutputTag<String>("InsertOrUpdateStream"){};
        //针对tms或者vehicle库中修改表结构的数据
        OutputTag<String> AlterStream = new OutputTag<String>("AlterStream"){};

        //对数据的处理
        SingleOutputStreamOperator<String> process = kafkaDStream.process(
                new ProcessFunction<String, String>() {
                    @Override
                        //每一条数据都会调用该方法
                        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                            //将String类型的数据转换为一个JSON对象
                            jsonObject = JSONObject.parseObject(value);
                            //取出json对象中的type字段，判断该数据是写入Hbase(InsertOrUpdate)还是MySQL(AlterOrDelete)
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
        ).setParallelism(4);//设置并行度为 4
        //输出到deleteSink
        process.getSideOutput(deleteStream).addSink(new MysqlSink()).name("deleteSink");
        //输出到HbaseSink
        process.getSideOutput(InsertOrUpdateStream).addSink(new HbaseOutPutForMat(NameSpace, prefix, suffix)).setParallelism(3).name("HbaseSink");
        //输出到AlterSink
        process.getSideOutput(AlterStream).addSink(new MysqlSink()).name("AlterSink");

        try {
            //执行任务
            env.execute("tms_job");
        } catch (Exception e) {
            //若是任务失败 打印出任务失败原因
            e.printStackTrace();
            //任务失败 发送邮件通知
            MailUtil.sendFailMail("Canal-Kafka-Hbase实时采集任务失败  请检查数据量是否过大！！！");
        }
    }

}
