package realtime;

import bean.PayMoney;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import util.ParseJsonData;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

class CkSinkBuilder implements JdbcStatementBuilder<PayMoney> {
    @Override
    public void accept(PreparedStatement ps, PayMoney payMoney) throws SQLException{
        ps.setString(1,payMoney.getUid());
        ps.setString(2,payMoney.getPaymoney());
        ps.setString(3,payMoney.getVip_id());
        ps.setString(4,payMoney.getUpdatetime());
        ps.setString(5,payMoney.getSiteid());
        ps.setString(6,payMoney.getDt());
        ps.setString(7,payMoney.getDn());
        ps.setString(8,payMoney.getCreatetime());
    }
}

class To_CK {
    public static void main(String[] args) {
        //获得环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); //设置并发为1，防止打印控制台乱序
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //Flink 默认使用 ProcessingTime 处理,设置成event time
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);//Table Env 环境
        //从Kafka读取数据
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "hadoop105:9092");
        pros.setProperty("group.id", "test");
        pros.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> sourceDs = env.addSource(new FlinkKafkaConsumer010<>("memberpaymoney", new SimpleStringSchema(), pros));
        String sql = "insert into dataCollectionTest values(?,?,?,?,?,?,?,?)";

        SingleOutputStreamOperator<PayMoney> mapDStream = sourceDs.map(
                new MapFunction<String, PayMoney>() {
                    @Override
                    public PayMoney map(String value) throws Exception {
                        JSONObject payMoneyJson = ParseJsonData.getJsonData(value);
                        PayMoney payMoney = new PayMoney();
                        payMoney.setUid(payMoneyJson.getString("uid"));
                        payMoney.setPaymoney(payMoneyJson.getString("paymoney"));
                        payMoney.setVip_id(payMoneyJson.getString("vip_id"));
                        payMoney.setUpdatetime(payMoneyJson.getString("updatetime"));
                        payMoney.setSiteid(payMoneyJson.getString("siteid"));
                        payMoney.setDt(payMoneyJson.getString("dt"));
                        payMoney.setDn(payMoneyJson.getString("dn"));
                        payMoney.setCreatetime(payMoneyJson.getString("createtime"));
                        System.out.println(payMoney.toString());
                        return payMoney;
                    }
                }
        );
//        mapDStream.addSink(JdbcSink.sink(
//        (sql,new CkSinkBuilder, new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:clickhouse://XX.XX.XX.XX:8123")
//                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
//                        .withUsername("default")
//                        .build();
//        ))
        mapDStream.addSink(
                JdbcSink.sink(
                        sql, new CkSinkBuilder(), new JdbcExecutionOptions.Builder().withBatchSize(1000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://hadoop105:8123/default")
                                .withUsername("")
                                .withPassword("")
                                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                .build()
                )
        );

        try {
            env.execute("输出ClickHouse");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
