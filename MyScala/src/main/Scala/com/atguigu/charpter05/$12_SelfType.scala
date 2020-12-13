package com.atguigu.charpter05

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.beans.BeanProperty

object $12_SelfType {

  /**
   * 自身类型：提醒子类要实现该trait的时候必须提前实现指定的trait/class
   */
  trait ReadAndWriteObject{
    _:Serializable =>

    /**
     * 从磁盘读取对象
     */
    def read() = {
      val ois = new ObjectInputStream(new FileInputStream("d:/obj.txt"))
      val obj = ois.readObject()
      ois.close()
      obj
    }

    /**
     * 对象写入磁盘
     */
    def write() = {
      val oos = new ObjectOutputStream(new FileOutputStream("d:/obj.txt"))

      oos.writeObject(this)

      oos.flush()

      oos.close()
    }

  }

  class Person(@BeanProperty val name:String,@BeanProperty val age:Int) extends ReadAndWriteObject with Serializable{

  }

  def main(args: Array[String]): Unit = {
    val person = new Person("zhangsan",20)
    person.write()

    val person2 = new Person("lisi",100)
    val obj = person2.read()

    //类型判断
    if(obj.isInstanceOf[Person]){
      //类型转换
      val person3 = obj.asInstanceOf[Person]
      println(person3.getAge)
      println(person3.getName)
    }

  }
}
