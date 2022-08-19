import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;



public class FlinkSQLCDC {


    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //设置检查点
        //env.getCheckpointConfig().setCheckpointStorage();
        //设置检查点每两秒一次
        //env.enableCheckpointing(2000);
        //设置两次checkpoint最小间隔时间
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv2 = TableEnvironment.create(settings);

        tableEnv2.createTemporarySystemFunction("if_null",ifnull_result.class);
        tableEnv2.createTemporarySystemFunction("replace",replace.class);
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
        //新增20张监听cdc表
        String CDCTable4 = "CREATE TABLE persons4 ( " +
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
                " 'table-name' = 'persons4'" +
                ")";
        tableEnv2.executeSql(CDCTable4);
//        String CDCTable5 = "CREATE TABLE persons5 ( " +
//                " id BIGINT primary key, " +
//                " sname STRING, " +
//                " stel BIGINT, " +
//                " ssex STRING, " +
//                " sprovince STRING " +
//                ") WITH ( " +
//                " 'connector' = 'mysql-cdc', " +
//                " 'scan.startup.mode' = 'latest-offset', " +
//                " 'hostname' = 'localhost', " +
//                " 'port' = '3306', " +
//                " 'username' = 'root', " +
//                " 'password' = '123456', " +
//                " 'database-name' = 'test', " +
//                " 'table-name' = 'persons5'" +
//                ")";
//        tableEnv2.executeSql(CDCTable5);
        String CDCTable6 = "CREATE TABLE persons6 ( " +
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
                " 'table-name' = 'persons6'" +
                ")";
        tableEnv2.executeSql(CDCTable6);
        String CDCTable7 = "CREATE TABLE persons7 ( " +
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
                " 'table-name' = 'persons7'" +
                ")";
        tableEnv2.executeSql(CDCTable7);
        String CDCTable8 = "CREATE TABLE persons8 ( " +
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
                " 'table-name' = 'persons8'" +
                ")";
        tableEnv2.executeSql(CDCTable8);
        String CDCTable9 = "CREATE TABLE persons9 ( " +
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
                " 'table-name' = 'persons9'" +
                ")";
        tableEnv2.executeSql(CDCTable9);
        String CDCTable10 = "CREATE TABLE persons10 ( " +
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
                " 'table-name' = 'persons10'" +
                ")";
        tableEnv2.executeSql(CDCTable10);
        String CDCTable11 = "CREATE TABLE persons11 ( " +
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
                " 'table-name' = 'persons11'" +
                ")";
        tableEnv2.executeSql(CDCTable11);

        //<editor-fold desc="sink mysql表">
        String SinkTable = "CREATE TABLE persons5 (" +
                " id BIGINT, " +
                " sname STRING, " +
                " stel STRING, " +
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
//        String aa="aa";
//        ifnull_result.null_result(aa);

        //3.查询数据并转换为流输出
        Table table = tableEnv2.sqlQuery("select *  from persons");
        Table table2 = tableEnv2.sqlQuery("select * from persons2");
        Table table3 = tableEnv2.sqlQuery("select * from persons3");
        Table table4 = tableEnv2.sqlQuery("select * from persons4");
//        Table table5 = tableEnv2.sqlQuery("select * from persons5");
        Table table6 = tableEnv2.sqlQuery("select * from persons6");
        Table table7 = tableEnv2.sqlQuery("select * from persons7");
        Table table8 = tableEnv2.sqlQuery("select * from persons8");
        Table table9 = tableEnv2.sqlQuery("select * from persons9");
        Table table10 = tableEnv2.sqlQuery("select * from persons10");
        Table table11 = tableEnv2.sqlQuery("select * from persons11");

        //注册视图
        tableEnv2.createTemporaryView("temp_table1",table);
        tableEnv2.createTemporaryView("temp_table2",table2);
        tableEnv2.createTemporaryView("temp_table3",table3);
        tableEnv2.createTemporaryView("temp_table4",table4);
//        tableEnv2.createTemporaryView("temp_table5",table5);
        tableEnv2.createTemporaryView("temp_table6",table6);
        tableEnv2.createTemporaryView("temp_table7",table7);
        tableEnv2.createTemporaryView("temp_table8",table8);
        tableEnv2.createTemporaryView("temp_table9",table9);
        tableEnv2.createTemporaryView("temp_table10",table10);
        tableEnv2.createTemporaryView("temp_table11",table11);
        tableEnv2.executeSql(createPrintOutDDL);
        //<editor-fold desc="ETL过程">
        String insertSql = "select t1.id,t1.sname,t1.stel,t1.ssex,t1.sprovince from temp_table1 t1 join (select * from temp_table2 t2 where t2.id<>'2') t2 " +
                "on t1.id=t2.id" +
                "join temp_table3 t3" +
                "on t1.id=t3.id" +
                "join temp_table4 t4" +
                "on t1.id=t4.id" +
                "join temp_table6 t6" +
                "on t1.id=t6.id" +
                "join temp_table7 t7" +
                "on t1.id=t7.id" +
                "join temp_table8 t8" +
                "on t1.id=t8.id" +
                "join temp_table9 t9" +
                "on t1.id=t9.id" +
                "join temp_table10 t10" +
                "on t1.id=t10.id" +
                "join temp_table11 t11" +
                "on t1.id=t11.id";
//        String insertQuery = "select t1.id" +
//                ",t1.sname" +
//                ",t1.stel " +
//                ",t1.ssex " +
//                ",t1.sprovince " +
//                "from temp_table1 t1  " +
//                "left join (select * from temp_table2 t2 where t2.id<>'2') t2 " +
//                "on t1.id=t2.id " +
//                "left join temp_table3 t3 " +
//                "on t1.id=t3.id left join temp_table4 t4 on t1.id=t4.id left join temp_table6 t6 on t1.id=t6.id " +
//                "left join temp_table7 t7 on t1.id=t7.id left join temp_table8 t8 on t1.id=t8.id " +
//                "left join temp_table9 t9 on t1.id=t9.id left join temp_table10 t10 on t1.id=t10.id left join temp_table11 t11 on t1.id=t11.id "
//                ;
        String insertQuery ="select t1.id,t1.sname,replace(cast(t1.stel as string),'1','2') as stel,concat(t1.ssex,'aa'),if_null(t1.sprovince) as sprovince from temp_table1 t1";
        //</editor-fold>
        //打印了就插不进去数据了
//        tableEnv2.sqlQuery(insertQuery).execute().print();
        Table table1 = tableEnv2.sqlQuery(insertQuery);
        //用动态表的方式建表

        table1.executeInsert("persons5").print();

        //4.启动
        env.execute("FlinkSQLCDC");

    }

}
