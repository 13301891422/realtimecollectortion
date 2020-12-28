package realtime;

import bean.PayMoney;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
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
import util.ParseJsonData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

class CkSinkBuilder implements JdbcStatementBuilder<PayMoney> {

    @Override
    public void accept(PreparedStatement ps, PayMoney payMoney) throws SQLException {
        if (null != payMoney.getUid()) {
            ps.setString(1, payMoney.getUid());
        }else {
            ps.setString(1, "");
        }

        if (null != payMoney.getPaymoney()) {
            ps.setString(2, payMoney.getPaymoney());
        }else {
            ps.setString(2, "");
        }

            ps.setString(3, payMoney.getVip_id());

            if (null != payMoney.getUpdatetime()) {
                ps.setString(4, payMoney.getUpdatetime());
            }else {
            ps.setString(4, "");
        }

        ps.setString(5, payMoney.getSiteid());
        ps.setString(6, payMoney.getDt());
        ps.setString(7, payMoney.getDn());
        ps.setString(8, payMoney.getCreatetime());
    }
}

public class To_CK {
    private static String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static String GROUP_ID = "group.id";
    private static String TOPIC = "topic";
    private static String SQL="sql";
    private static String DATABASE= "database";
    private static String START_FROMTIMESTAMP="start_fromtimestamp";



    private static final Logger log = LoggerFactory.getLogger(To_CK.class);

    public static void main(String[] args)  {
        //获得环境
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1); //设置并发为1，防止打印控制台乱序
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //Flink 默认使用 ProcessingTime 处理,设置成event time
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);//Table Env 环境
        //从Kafka读取数据
        Properties pros = new Properties();
//        pros.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
//        pros.setProperty(GROUP_ID, params.get(GROUP_ID));
        pros.setProperty("bootstrap.servers","192.168.20.27:9092");
        pros.setProperty("group.id","test");

        pros.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>
                (
                        "memberpaymoney",
//                        params.get(TOPIC),
                        new SimpleStringSchema(),
                        pros);

//        consumer.setStartFromTimestamp(1608739200000L);   //从kafka的何时时间点进行消费

        DataStreamSource<String> sourceDs = env.addSource(consumer);
        String sql = "insert into dataCollectionTest values(?,?,?,?,?,?,?,?)";


        SingleOutputStreamOperator<PayMoney> mapDStream = sourceDs.map(
                    new MapFunction<String, PayMoney>() {
                        @Override
                        public PayMoney map(String value) throws Exception {
                            JSONObject payMoneyJson = ParseJsonData.getJsonData(value);
                            PayMoney payMoney = new PayMoney();
                            try {
                                payMoney.setUid(payMoneyJson.getString("uid"));
                                payMoney.setPaymoney(payMoneyJson.getString("paymoney"));
                                payMoney.setVip_id(payMoneyJson.getString("vip_id"));
                                payMoney.setUpdatetime(payMoneyJson.getString("updatetime"));
                                payMoney.setSiteid(payMoneyJson.getString("siteid"));
                                payMoney.setDt(payMoneyJson.getString("dt"));
                                payMoney.setDn(payMoneyJson.getString("dn"));
                                payMoney.setCreatetime(payMoneyJson.getString("createtime"));
                            } catch (Exception e) {
//                                log.error("kafka输入数据异常",e);
                                System.out.println("kafka输入数据异常");
                            }

                            return payMoney;
                        }
                    }
            ).name("Transform JavaBean");



//        mapDStream.addSink(JdbcSink.sink(
//        (sql,new CkSinkBuilder, new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:clickhouse://XX.XX.XX.XX:8123")
//                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
//                        .withUsername("default")
//                        .build();
//        ))

        mapDStream
                .addSink(JdbcSink
                        .sink(
//                              params.get(SQL),
                                sql,
                                new CkSinkBuilder(), new JdbcExecutionOptions
                                        .Builder()
                                        .withBatchSize(100)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                                        .withUrl("jdbc:clickhouse://hadoop105:8123/default")
//                                        .withUrl("jdbc:clickhouse://101.37.247.143:8123/"+params.get(DATABASE))
                                        .withUrl("jdbc:clickhouse://101.37.247.143:8123/default")
                                        .withUsername("")
                                        .withPassword("")
                                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                        .build()
                        )
                ).name("ToClickHouse");


        try {
            env.execute("输出ClickHouse");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
