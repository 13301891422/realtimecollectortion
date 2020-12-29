//package realtime;
//
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Admin;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Table;
//import java.util.List;
//
//class HBaseSinkFunction extends RichSinkFunction<List<Put>> {
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        HbaseUtils.connectHbase();
//        TableName table = TableName.valueOf(Constants.tableNameStr);
//        Admin admin = HbaseUtils.connection.getAdmin();
//        if (!admin.tableExists(table)) {
//            HTableDescriptor tableDescriptor = new HTableDescriptor(Constants.tableNameStr);
//            tableDescriptor.addFamily(new HColumnDescriptor(Constants.columnFamily));
//            admin.createTable(tableDescriptor);
//        }
//    }
//
//    @Override
//    public void invoke(List<Put> putList, Context context) throws Exception {
//
//        Table table = HbaseUtils.connection.getTable(TableName.valueOf(Constants.tableNameStr));
//        table.put(putList);
//    }
//
//    @Override
//    public void close() throws Exception {
//        super.close();
//        HbaseUtils.closeHBaseConnect();
//    }
//}
