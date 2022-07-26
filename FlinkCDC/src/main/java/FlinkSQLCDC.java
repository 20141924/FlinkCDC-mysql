import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;


public class FlinkSQLCDC {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv2 = TableEnvironment.create(settings);
        //initial 这个适用于一开始的全量初始化，这个不会动态变化
        //latest-offset  这个的话会动态的变化，增量的
        //2.使用flinkSQL DDL模式构建CDC 表
        //<editor-fold desc="监听cdc表1">
      String CDCTable1= "CREATE TABLE persons ( " +
                " id BIGINT primary key, " +
                " sname STRING, " +
                " stel BIGINT, " +
                " ssex STRING, " +
                " sprovince STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'hostname' = 'localhost', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'database-name' = 'test', " +
                " 'table-name' = 'persons'" +
                ")";
        //</editor-fold>
        tableEnv2.executeSql(CDCTable1);
        //<editor-fold desc="监听cdc表二">
        String CDCTable2 = "CREATE TABLE persons2 ( " +
                " id BIGINT primary key, " +
                " sname STRING, " +
                " stel BIGINT, " +
                " ssex STRING, " +
                " sprovince STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'hostname' = 'localhost', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'database-name' = 'test', " +
                " 'table-name' = 'persons2'" +
                ")";
        //</editor-fold>
        tableEnv2.executeSql(CDCTable2);
        //<editor-fold desc="监听cdc表三">
        String CDCTable3 = "CREATE TABLE persons3 ( " +
                " id BIGINT primary key, " +
                " sname STRING, " +
                " stel BIGINT, " +
                " ssex STRING, " +
                " sprovince STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'hostname' = 'localhost', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'database-name' = 'test', " +
                " 'table-name' = 'persons3'" +
                ")";
        //</editor-fold>
        tableEnv2.executeSql(CDCTable3);
        //<editor-fold desc="sink mysql表">
        String SinkTable = "CREATE TABLE persons5 (" +
                " id BIGINT, " +
                " sname STRING, " +
                " stel BIGINT, " +
                " ssex STRING, " +
                " sprovince STRING, " +
                " primary key (id) not enforced" +
                ") WITH (" +
                " 'connector' = 'jdbc', " +
                " 'url' = 'jdbc:mysql://localhost:3306/test2?serverTimezone=UTC', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'table-name' = 'persons5' " +
                ")";
        //</editor-fold>
        String createPrintOutDDL = SinkTable;

        //3.查询数据并转换为流输出
        Table table = tableEnv2.sqlQuery("select * from persons");
        Table table2 = tableEnv2.sqlQuery("select * from persons2");
        Table table3 = tableEnv2.sqlQuery("select * from persons3");
        //注册视图
        tableEnv2.createTemporaryView("temp_table1",table);
        tableEnv2.createTemporaryView("temp_table2",table2);
        tableEnv2.createTemporaryView("temp_table3",table3);
        tableEnv2.executeSql(createPrintOutDDL);
        //<editor-fold desc="ETL过程">
        String insertQuery = "select t1.id" +
                                    ",t1.sname" +
                                    ",t1.stel " +
                                    ",t1.ssex " +
                                    ",t1.sprovince " +
                                    "from temp_table1 t1  " +
                                    "join (select * from temp_table2 t2 where t2.id<>'2') t2 " +
                                    "on t1.id=t2.id " +
                                    "join temp_table3 t3 " +
                                    "on t1.id=t3.id"
                ;
        //</editor-fold>
        Table table1 = tableEnv2.sqlQuery(insertQuery);
        //用动态表的方式建表

        table1.executeInsert("persons5").print();

        //4.启动
        env.execute("FlinkSQLCDC");

    }

}
