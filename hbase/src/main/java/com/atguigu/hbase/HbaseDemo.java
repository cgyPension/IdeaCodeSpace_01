package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author GyuanYuan Cai
 * 2020/8/21
 * Description:
 */

public class HbaseDemo {

   private Connection connection;
   private Admin admin;
   /**
    * 初始化
    */
   @Before
   public void init() throws IOException {
      //1、获取hbase连接
      Configuration conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop104:2181");
      connection = ConnectionFactory.createConnection(conf);
      //2、创建Admin
      admin = connection.getAdmin();
   }


   /**
    * 创建命名空间
    */
   @Test
   public void createNameApace() throws IOException {
       //3、创建命名空间
      NamespaceDescriptor namespaceDescriptor= NamespaceDescriptor.create("big2").build();
      admin.createNamespace(namespaceDescriptor);
      //4、关闭连接
      admin.close();
      connection.close();
   }


   /**
    * 显示所有的namespace
    */
   @Test
   public void listNameSpace() throws IOException {
      NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
      for (NamespaceDescriptor namespace : namespaceDescriptors) {
         System.out.println(namespace.getName());
      }
   }

   /**
    *查看命名空间所有表
    */
   @Test
   public void listNameSpaceTables() throws Exception {
      final List<TableDescriptor> tableDescriptors = admin.listTableDescriptorsByNamespace("big".getBytes());
      for (TableDescriptor tableDescriptor : tableDescriptors) {
         System.out.println(new String(tableDescriptor.getTableName().getName()));
      }
   }

   /**
    *删除命名空间
    */
   @Test
   public void dropNameSpace() throws Exception{
      //1、获取命名空间所有表
      final List<TableDescriptor> tableDescriptors = admin.listTableDescriptorsByNamespace("big2".getBytes());
      for (TableDescriptor tableDescriptor:tableDescriptors){
         //2、禁用表
         admin.disableTable(tableDescriptor.getTableName());

         //3、删除表
         admin.deleteTable(tableDescriptor.getTableName());
      }

      //4、删除命名空间
      admin.deleteNamespace("big2");
   }

   /**
    *创建表
    */
   @Test
   public void createTable() throws IOException {
       //3、创建表
      //create 表名，列族,..
      //创建列族的描述
      ColumnFamilyDescriptor base_info = ColumnFamilyDescriptorBuilder.newBuilder("base_info".getBytes()).build();
      ColumnFamilyDescriptor extra_info = ColumnFamilyDescriptorBuilder.newBuilder("extra_info".getBytes()).build();
      //创建表的描述
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("big:person"))
              //将列族关联到表
         .setColumnFamily(base_info)
         .setColumnFamily(extra_info)
         .build();
      //预区域
      //final byte[][] splitKeys = {"10".getBytes(), "20".getBytes(), "30".getBytes()};
      //admin.createTable(tableDescriptor,splitKeys);
      admin.createTable(tableDescriptor);
      //4、关闭连接
   }
   /**
    *修改表
    */
   @Test
   public void t() throws IOException {
      //修改extra_info列族的版本数
      ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("extra_info".getBytes()).setMinVersions(2).setMaxVersions(2).build();
      ColumnFamilyDescriptor familyDescriptor1 = ColumnFamilyDescriptorBuilder.newBuilder("base_info".getBytes()).setMinVersions(2).setMaxVersions(2).build();
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("user")).setColumnFamily(familyDescriptor).setColumnFamily(familyDescriptor1).build();
      admin.modifyTable(tableDescriptor);
   }
   /**
    *显示所有表
    */
   @Test
   public void listTable() throws IOException {
      TableName[] tableNames = admin.listTableNames();
      for (TableName tableName : tableNames) {
         System.out.println(new String(tableName.getName()));
      }
   }
   /**
    *删除表
    */
   @Test
   public void dropTable() throws IOException {
       //1、禁用表
      admin.disableTable(TableName.valueOf("user"));
      //2、删除表
      admin.deleteTable(TableName.valueOf("user"));
   }

   /**
    *插入数据
    */
   @Test
   public void put() throws IOException {
       //1、创建Table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、插入数据
      Put put = new Put("1000".getBytes());
      put.addColumn("base_info".getBytes(),"name".getBytes(),"zhangsan".getBytes());
      put.addColumn("base_info".getBytes(),"age".getBytes(), Bytes.toBytes(20));
      table.put(put);
      //3、关闭Table
      table.close();
   }

   /**
    *批量插入数据
    */
   @Test
   public void putList() throws IOException {
       //1、需要创建Table
      Table table = connection.getTable(TableName.valueOf("big:person"));
      ArrayList<Put> puts = new ArrayList<>();
      Put put = null;
      for (int i = 0; i <=10; i++) {
         new Put(("1001"+i).getBytes());
         put.addColumn("base_info".getBytes(),"name".getBytes(),("zhangsan-"+i).getBytes());
         put.addColumn("base_info".getBytes(),"age".getBytes(),Bytes.toBytes(20+i));
         put.addColumn("extra_info".getBytes(),"address".getBytes(),("shenzhen_"+i).getBytes());
         puts.add(put);
      }
      //2、关闭Table
      table.close();
   }

   /**
    *通过rowkey查询整行数据
    */
   @Test
   public void get1() throws IOException {
       //1、获取Table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、查询
      Get get = new Get("1000".getBytes());
      Result result = table.get(get);
      //3、显示数据
      List<Cell> cells = result.listCells();
      for (Cell cell : cells) {
         //Cell 列族，列限定，值，
         String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
         String qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
         String rowkey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getValueLength());
         if (family.equals("base_info") && qualifier.equals("age")) {
            int value = Bytes.toInt(cell.getValueArray(), cell.getQualifierOffset(), cell.getValueLength());
            System.out.println(rowkey + "--" + family + "---" + qualifier + "--" + value);
         } else {
            String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(rowkey + "--" + family + "---" + qualifier + "--" + value);
         }

      }
      //4、关闭Table对象
      table.close();
   }

   /**
    *获取某个列族或者列的数据
    */
   @Test
   public void get2() throws IOException {
       //1、获取table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、查询
      Get get = new Get("10010".getBytes());
      //查询指定列族
      get.addFamily("base_info".getBytes());

      get.addColumn("extra_info".getBytes(),"adress".getBytes());
      Result result = table.get(get);
      //3、展示结果
      List<Cell> cells = result.listCells();
      for (Cell cell : cells) {
         byte[] familyByte = CellUtil.cloneFamily(cell);
         String family = new String(familyByte);
         String rowkey = new String(CellUtil.cloneRow(cell));
         String qualifier = new String(CellUtil.cloneQualifier(cell));
         if (family.equals("base_info") && qualifier.equals("age")) {
            int value = Bytes.toInt(CellUtil.cloneValue(cell));
            System.out.println(rowkey + "--" + family + "---" + qualifier + "--" + value);
         } else {
            String value = new String(CellUtil.cloneValue(cell));
            System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
         }
      }
      //4、关闭
      table.close();
   }
   
   /**
    *批量查询
    */
   @Test
   public void getBath() throws IOException {
       //1、获取table
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、查询
      ArrayList<Get> gets = new ArrayList<>();
      Get get = null;
      for (int i = 3; i <=6; i++) {
          get=new Get(("1001"+i).getBytes());
         get.addFamily("base_info".getBytes());
         gets.add(get);
      }
      //多条数据
      Result[] results = table.get(gets);
      for (Result result : results) {
         List<Cell> cells = result.listCells();
         for (Cell cell : cells) {
            byte[] familyByte = CellUtil.cloneFamily(cell);
            String family = new String(familyByte);
            String rowkey = new String(CellUtil.cloneRow(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            if (family.equals("base_info") && qualifier.equals("age")) {
               final int value = Bytes.toInt(CellUtil.cloneValue(cell));
               System.out.println(rowkey + "--" + family + "---" + qualifier + "--" + value);
            } else {
               String value = new String(CellUtil.cloneValue(cell));
               System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
            }
         }
      }
      //4、关闭
      table.close();
   }


   /**
    *扫描数据
    */
   public void scan() throws Exception {
      //1、获取Table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、查询数据
      // 查询全部数据
      //Scan scan = new Scan();
      //查询某个列族数据
      Scan scan = new Scan();
      scan.addFamily("base_info".getBytes());
      ResultScanner scanner = table.getScanner(scan);
      //3、展示数据
      Iterator<Result> iterator = scanner.iterator();
      while (iterator.hasNext()) {
         Result rs = iterator.next();
         List<Cell> cells = rs.listCells();
         for (Cell cell : cells) {
            byte[] familyByte = CellUtil.cloneFamily(cell);
            String family = new String(familyByte);
            String rowkey = new String(CellUtil.cloneRow(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            if (family.equals("base_info") && qualifier.equals("age")) {
               int value = Bytes.toInt(CellUtil.cloneValue(cell));
               System.out.println(rowkey + "--" + family + "---" + qualifier + "--" + value);
            } else {
               String value = new String(CellUtil.cloneValue(cell));
               System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
            }
         }
      }
      //4、关闭
      table.close();
   }

   /**
    * 根据值来查询
    * select*from xx where age=25
    */
   @Test
   public void filterByValue() throws IOException {
       //1、获取Table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、查询数据
      //查询全部数据
      //Scan scan = new Scan();
      //查询某个列族数据
      Scan scan = new Scan();
      //根据value进行过滤，只显示单个cell
      //BinaryComparator comparator = new BinaryComparator(Bytes.toBytes(25));
      //ValueFilter filter = new ValueFilter(CompareOperator.EQUAL, comparator);
      //scan.setFilter(filter);
      //根据value进行过滤，显示整行数据
      SingleColumnValueFilter valueFilter = new SingleColumnValueFilter("base_info".getBytes(), "age".getBytes(), CompareOperator.EQUAL, Bytes.toBytes(25));
      scan.setFilter(valueFilter);
      ResultScanner scanner = table.getScanner(scan);
      //3、展示数据
      Iterator<Result> iterator = scanner.iterator();
      while (iterator.hasNext()) {
         Result rs = iterator.next();
         List<Cell> cells = rs.listCells();
         for (Cell cell : cells) {
            byte[] familyByte = CellUtil.cloneFamily(cell);
            String family = new String(familyByte);
            String rowkey = new String(CellUtil.cloneRow(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            if (family.equals("base_info") && qualifier.equals("age")) {
               int value = Bytes.toInt(CellUtil.cloneValue(cell));
               System.out.println(rowkey + "--" + family + "---" + qualifier + "--" + value);
            } else {
               String value = new String(CellUtil.cloneValue(cell));
               System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
            }
         }

      }
      //4、关闭
      table.close();
   }


   /**
    *select*from xx where name like ‘%z%’
    */
   @Test
   public void filterByLike() throws IOException {
       //1、获取Table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、查询数据
      //查询某个列族数据
      Scan scan = new Scan();
      SubstringComparator comparator = new SubstringComparator("-5");
      //ValueFilter filter = new ValueFilter(CompareOperator.EQUAL, comparator);
      //scan.setFilter(filter);
      SingleColumnValueFilter filter = new SingleColumnValueFilter("base_info".getBytes(), "name".getBytes(), CompareOperator.EQUAL, comparator);
      scan.setFilter(filter);
      ResultScanner scanner = table.getScanner(scan);
      //3、展示数据
      Iterator<Result> iterator = scanner.iterator();
      while (iterator.hasNext()) {
         Result rs = iterator.next();
         List<Cell> cells = rs.listCells();
         for (Cell cell : cells) {
            byte[] familyByte = CellUtil.cloneFamily(cell);
            String family = new String(familyByte);
            String rowkey = new String(CellUtil.cloneRow(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            if (family.equals("base_info")&&qualifier.equals("age")) {
               int value = Bytes.toInt(CellUtil.cloneValue(cell));
               System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
            }else{
               String value = new String(CellUtil.cloneValue(cell));
               System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
            }
         }
      }
      //4、关闭
      table.close();
   }

   /**
    *select*from xx where name like '%san%' and (age>20 and name='zhangsan-4')
    */
   @Test
   public void filterByMuti() throws IOException {
       //1、获取Table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、查询数据
      //查询某个列族数据
      Scan scan = new Scan();
      //name like '%san%'
      SingleColumnValueFilter like = new SingleColumnValueFilter("base_info".getBytes(), "name".getBytes(), CompareOperator.EQUAL, new SubstringComparator("scan"));
      //age>20
      SingleColumnValueFilter age = new SingleColumnValueFilter("base_info".getBytes(), "age".getBytes(), CompareOperator.GREATER, Bytes.toBytes(20));
      //name='zhangsan-4'
      SingleColumnValueFilter name = new SingleColumnValueFilter("base_info".getBytes(), "name".getBytes(), CompareOperator.EQUAL, Bytes.toBytes("zhangsan-4"));

      //(age>20 or name='zhangsan-4')
      FilterList nameAndAge = new FilterList(FilterList.Operator.MUST_PASS_ONE);
      nameAndAge.addFilter(name);
      nameAndAge.addFilter(age);

      //name like '%san%' and (age>20 or name='zhangsan-4')
      FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filterList.addFilter(like);
      filterList.addFilter(nameAndAge);
      scan.setFilter(filterList);
      ResultScanner scanner = table.getScanner(scan);
      //3、展示数据
      Iterator<Result> iterator = scanner.iterator();
      while (iterator.hasNext()){
         Result rs = iterator.next();
         List<Cell> cells = rs.listCells();
         for(Cell cell: cells){
            byte[] familyByte = CellUtil.cloneFamily(cell);
            String family = new String(familyByte);
            String rowkey = new String(CellUtil.cloneRow(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            if(family.equals("base_info")&& qualifier.equals("age")){
               final int value = Bytes.toInt(CellUtil.cloneValue(cell));
               System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
            }else{
               String value = new String(CellUtil.cloneValue(cell));
               System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
            }
         }
      }
      //4、关闭
      table.close();
   }

   @Test
   public void rangeScan() throws Exception{
      //1、获取Table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、查询数据
      //查询某个列簇数据
      Scan scan = new Scan();
      //根据列查询
      // base_info:1  base_info:2
      ColumnRangeFilter filter = new ColumnRangeFilter("base_info:name".getBytes(),true,null,false);
      scan.setFilter(filter);
      ResultScanner scanner = table.getScanner(scan);
      //3、展示数据
      Iterator<Result> iterator = scanner.iterator();
      while (iterator.hasNext()){
         Result rs = iterator.next();
         List<Cell> cells = rs.listCells();
         for(Cell cell: cells){
            byte[] familyByte = CellUtil.cloneFamily(cell);
            String family = new String(familyByte);
            String rowkey = new String(CellUtil.cloneRow(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            if(family.equals("base_info")&& qualifier.equals("age")){
               final int value = Bytes.toInt(CellUtil.cloneValue(cell));
               System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
            }else{
               String value = new String(CellUtil.cloneValue(cell));
               System.out.println(rowkey+"--"+family+"---"+qualifier+"--"+value);
            }
         }
      }
      //4、关闭
      table.close();
   }


   /**
    *删除数据
    */
   @Test
   public void delete() throws IOException {
       //1、获取table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、删除
      Delete delete = new Delete("1000".getBytes());
      table.delete(delete);
      //3、关闭
      table.close();
   }

   /**
    *批量删除
    */
   @Test
   public void deleteBatch() throws IOException {
       //1、获取table对象
      Table table = connection.getTable(TableName.valueOf("big:person"));
      //2、删除
      ArrayList<Delete> deletes = new ArrayList<>();
      Delete delete = null;
      for (int i = 4; i <=7; i++) {
          delete = new Delete(("1001" + i).getBytes());
          deletes.add(delete);
      }
      table.delete(delete);
      //3、关闭
      table.close();
   }

   /**
    *关闭
    */
   @After
   public void close() throws IOException {
      if (admin!=null) admin.close();
      if(connection!=null) connection.close();
   }
}


/**
 * Description:
 */

