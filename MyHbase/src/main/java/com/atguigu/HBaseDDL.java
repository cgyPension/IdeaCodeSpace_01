package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseDDL {
    public static void main(String[] args) throws IOException {
        System.out.println(isTableExist("bigdata:student1"));
    }
    public static boolean isTableExist(String tableName) throws IOException {
        // 创建配置信息并配置
        final Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        // 获取与HBase的连接
        final Connection connection = ConnectionFactory.createConnection(configuration);

        // 获取DDL操作对象
        final Admin admin = connection.getAdmin();

        // 判断表是是否存在
        final boolean tableExists = admin.tableExists(TableName.valueOf(tableName));

        // 关闭连接
        admin.close();
        connection.close();
        return tableExists;
    }
}
