package flinktohbase;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2021/1/5 9:15:42
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MysqlSink extends RichSinkFunction<String> {
    private static SimpleDateFormat sdf;
    private static String deleteTime_TimeStamp ;
    private static PreparedStatement preparedStatement_Alter ;
    private static PreparedStatement preparedStatement_Delete ;
    private static Connection connection;
    private static String primaryKey;


    /**
     * open方法是初始化方法，会在invoke方法之前执行，执行一次。
     */

    @Override
    public void open(Configuration parameters) throws Exception {
        // JDBC连接信息
//        String USERNAME = "root";
//        //生产的PASSWORD
//        //String PASSWORD = "123456";
//        String PASSWORD = "root";
////      String DBURL = "jdbc:mysql://172.16.182.171:3306/data_service";
//        String DBURL = "jdbc:mysql://localhost:3306/data_service";
//        // 加载JDBC驱动
        Class.forName("com.mysql.jdbc.Driver");
        // 获取数据库连接
//        connection = DriverManager.getConnection(DBURL, USERNAME, PASSWORD);
        connection = DataSourceUtil.getConnection();
//        String sql = "insert into flink_test values (?)";
        preparedStatement_Alter =
                connection.prepareStatement("INSERT INTO  alter_table VALUES(?,?,?,?)");
        preparedStatement_Delete =
                connection.prepareStatement("INSERT INTO  delete_data_table VALUES(?,?,?,?)");
        super.open(parameters);
    }

    //每来一条数据都会调用该方法
    @Override
    public void invoke(String value, Context context) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String type = jsonObject.getString("type");
        switch (type){
            case "DELETE": invokeIntoDelete(value);break;
            case "ALTER" : invokeIntoAlter(value) ;break;
            default:;break;
        }
    }

    public void invokeIntoAlter(String value){
        JSONObject jsonObject = JSONObject.parseObject(value);
        String db_name = jsonObject.getString("database");
        String tb_name = jsonObject.getString("table");
        String timeStamp = jsonObject.getString("ts");
        String querySql = jsonObject.getString("sql");
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String create_time = sdf.format(Long.parseLong(String.valueOf(timeStamp)));
        try {
            preparedStatement_Alter.setString(1, db_name);
            preparedStatement_Alter.setString(2, tb_name);
            preparedStatement_Alter.setString(3, querySql);
            preparedStatement_Alter.setString(4, create_time);
            preparedStatement_Alter.execute();
//          DataSourceUtil.closePrepareStatement(preparedStatement_Alter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void invokeIntoDelete(String value) throws SQLException {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String db_name = jsonObject.getString("database");
        String tb_name = jsonObject.getString("table");
        String deleteTime_TimeStamp = jsonObject.getString("es");
        String data = jsonObject.getString("data");
        JSONArray dataArray = JSONArray.parseArray(data);
        for (Object o : dataArray.toArray()) {
            //  jsonObject的数据格式为:
            // {"no":"aaa","school":"2200","class":"222","address":"333","name":"xxx","teacher":"oo","age":"30","shijian":"null"}
            JSONObject data_json = JSONObject.parseObject(JSON.toJSONString(o));
            String waybill_code = data_json.getString("waybill_code");
            String id = data_json.getString("id");
            String code = data_json.getString("code");
            String dispatch_code = data_json.getString("dispatch_code");
            if(StringUtils.isNotBlank(waybill_code)){
                primaryKey = waybill_code;
            }else if(StringUtils.isNotBlank(dispatch_code)){
                primaryKey = dispatch_code;
            }else if(StringUtils.isNotBlank(id)){
                primaryKey = id;
            }else if(StringUtils.isNotBlank(code)){
                primaryKey = code;
            }else{
                primaryKey = "noPrimaryKey";
            }
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String delete_time = sdf.format(new Date(Long.parseLong(deleteTime_TimeStamp)));
            preparedStatement_Delete.setString(1,db_name);
            preparedStatement_Delete.setString(2,tb_name);
            preparedStatement_Delete.setString(3,primaryKey);
            preparedStatement_Delete.setString(4,delete_time);
            preparedStatement_Delete.execute();
        }

//        DataSourceUtil.closePrepareStatement(preparedStatement_Delete);
    }

    @Override
    public void close() throws Exception {
        DataSourceUtil.closeConnection(connection);
    }
}

