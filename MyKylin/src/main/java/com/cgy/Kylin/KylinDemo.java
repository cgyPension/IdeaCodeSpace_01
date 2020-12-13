package com.cgy.Kylin;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author GyuanYuan Cai
 * 2020/9/30
 * Description:
 */

public class KylinDemo {
    public static void main(String[] args) throws Exception{

        //Kylin_URL
        String url = "jdbc:kylin://hadoop102:7070/gmall";

        //Kylin_JDBC 驱动
        Class.forName("org.apache.kylin.jdbc.Driver");

        //获取连接
        Connection connection = DriverManager.getConnection(url, "ADMIN", "KYLIN");

        //预编译SQL
        PreparedStatement ps = connection.prepareStatement("select sum(final_amount_d) from DWD_FACT_ORDER_DETAIL group by USER_ID");

        //执行查询
        ResultSet resultSet = ps.executeQuery();

        //遍历打印
        while (resultSet.next()){
            System.out.println(resultSet.getInt(1));
        }

        resultSet.close();
        ps.close();
        connection.close();

}


/**
 * Description:
 */

