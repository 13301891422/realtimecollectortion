package realtime;

import bean.PayMoney;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import util.ParseJsonData;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2020/12/24 9:24:29
 */
public class FlinkRealTimeCollection {
    private static String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static String GROUP_ID = "group.id";
    private static String RETRIES = "retries";
    private static String TOPIC = "topic";



    public static void main(String[] args) {
        String tablename = "dataCollectionTest";
        String username = "";
        String password = "";
        String[] ips = {"101.37.247.143"};
        String[] tableColums = {"uid","paymoney","vip_id","updatetime","siteid","dt","dn","createtime"};
        List<String> types = new ArrayList<>();
        String[] columns = null;

        Properties props = new Properties();
//      props.setProperty("zookeeper.connect", ZOOKEEPER_HOST);
        props.setProperty("bootstrap.servers", "192.168.20.27:9092");
        props.setProperty("group.id", "test-consumer-group");
//        ParameterTool.fromArgs();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
        DataStreamSource<String> sourceDs = env.addSource(new FlinkKafkaConsumer010<String>("memberpaymoney", new SimpleStringSchema(), props));
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
                        return payMoney;
                    }
                }
        );

        mapDStream.map(
                new MapFunction<PayMoney, String >() {
                    @Override
                    public String map(PayMoney value) throws Exception {
                        System.out.println(value.toString());
                        return value.toString();
                    }
                }
        );

        mapDStream.addSink(new ClickhouseSink(tablename,username,password,ips,tableColums,types,columns));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
