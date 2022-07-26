# FlinkCDC-mysql
FlinkSQLCDC.java文件通过监听mysql多张表的日志，做完清洗以后，写入到mysql
实现功能：
1，实现监听mysql的binlog日志
2，使用sql清洗数据
3，使用upset的形式写入到mysql
注意点：
监听biglog日志的表要有主键才能实现upset的功能

测试程序：使用Python文件模拟插入mysql，监听这些表的biglog,可以用来做压测
