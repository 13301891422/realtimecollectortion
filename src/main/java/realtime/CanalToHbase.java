package realtime;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.ParseJsonData;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CanalToHbase {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8); //设置并发为1，防止打印控制台乱序
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //Flink 默认使用 ProcessingTime 处理,设置成event time
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);//Table Env 环境
        //从Kafka读取数据
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "hadoop105:9092");
        pros.setProperty("group.id", "test");
        pros.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>
                ("memberpaymoney", new SimpleStringSchema(), pros);

        DataStreamSource<String> sourceDs = env.addSource(consumer);


        KeyedStream<String, String> keyedStream =
                sourceDs.keyBy(value -> ParseJsonData.getJsonData(value).getString("tablename"));



        keyedStream.countWindow(100).apply(new AllWindowFunction<String, List<Put>, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow window, Iterable<String> messages, Collector<List<Put>> out) throws Exception {
                List<Put> putList=new ArrayList<>();
                for (String value : messages)
                {
                    String rowKey=value.replace("::","_");
                    Put put = new Put(Bytes.toBytes(rowKey.toString()));
                    String[] column=value.split("::");
                    for (int i = 0; i < column.length; i++) {
                        put.addColumn(
                                Bytes.toBytes(Constants.columnFamily),
                                Bytes.toBytes(Constants.columnArray[i]),
                                Bytes.toBytes(column[i]));
                    }
                    putList.add(put);
                }
                out.collect(putList);
            }
        });

    }
}
