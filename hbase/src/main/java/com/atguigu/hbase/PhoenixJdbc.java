package com.atguigu.hbase;

import org.apache.phoenix.queryserver.client.ThinClientUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * @author GyuanYuan Cai
 * 2020/8/22
 * Description:
 */

public class PhoenixJdbc {

    private Connection connection = null;
    private PreparedStatement statement = null;

    @Before
    public void init() throws ClassNotFoundException, SQLException {
        //1、加载驱动
        Class.forName("org.apache.phoenix.queryserver.client.Driver");
        //2、获取connection连接
        String url = ThinClientUtil.getConnectionUrl("hadoop102", 8765);
        System.out.println(url);
        connection = DriverManager.getConnection(url);
        //设置自动提交
        connection.setAutoCommit(true);
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() throws SQLException {
        //3、创建Statement对象
        //select*from where name = 'zhangsan' or 1=1
        //connection.createStatement();
        String sql = "create table person("+
                "id varchar primary key,"+
                "name varchar,"+
                "age varchar)COLUMN_ENCODED_BYTES=0";
        statement = connection.prepareStatement(sql);
        //4、执行sql
        statement.execute();
    }

    /**
     *插入数据
     */
    @Test
    public void upsert() throws SQLException {
        //1、获取statement
        String sql ="upsert into person values(?,?,?)";
        statement = connection.prepareStatement(sql);
        //2、赋值
        statement.setString(1,"1001");
        statement.setString(2,"zhangsan");
        statement.setString(3,"20");
        //3、执行
        statement.executeUpdate();
        //提交
        //connection.commit();
    }

    /**
     *批次插入
     */
    @Test
    public void upsertBatch() throws SQLException {
        //1、获取statement
        String sql = "upsert into person values(?,?,?)";
        statement = connection.prepareStatement(sql);
        //2、赋值
        int i =  130;
        while (i<=250) {
            System.out.println("---------------------");
            statement.setString(1,"100"+i);
            statement.setString(2,"zhangsan"+i);
            statement.setString(3,"2"+i);
            statement.addBatch();
            if (i%5==0) {
                System.out.println("++++++++++++++++++++++++++");
                //提交一个批次
                statement.executeBatch();
                statement.clearBatch();
                connection.commit();
            }
            i=++i;
        }
        //提交最后一个不满50的批次
        statement.executeBatch();
        connection.commit();
        System.out.println("===============================");
    }


    /**
     *删除数据
     */
    @Test
    public void delete() throws SQLException {
        String sql = "delete from person where id=?";
        statement = connection.prepareStatement(sql);
        statement.setString(1,"10099");
        statement.executeUpdate();
    }

    /**
     *查询数据
     */
    @Test
    public void query() throws SQLException {
        //1、获取statement对象
        String sql = "select*from person where id>?";
        statement = connection.prepareStatement(sql);
        //2、给参数赋值
        statement.setString(1,"1008");
        //3、执行查询
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            String age = resultSet.getString("age");
            System.out.println("id="+id+",name="+name+",age="+age);
        }
        //4、结果展示
    }

    @Test
    public void dropTable() throws SQLException {
        String sql = "drop table person";
        statement = connection.prepareStatement(sql);
        statement.execute();
    }

    @After
    public void close() throws SQLException {
        //5、关闭
        if (statement!=null) statement.close();
        if (connection!=null) connection.close();
    }
}





















































