package com.daslab.czy.HBase

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInput, ObjectInputStream, ObjectOutputStream}

object SerializeUtils {

  /**
   * 序列化
   * @param obj
   * @return
   */
  def serialize(obj: Object): Array[Byte] = {
    try {
      val baos = new ByteArrayOutputStream
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(obj)
      val array: Array[Byte] = baos.toByteArray
      oos.close()
      baos.close()
      array
    }catch{
      case  e: Exception =>
        println(e)
        null
    }
  }

  /**
   * 反序列化
   * @param bytes
   * @return
   */
  def  deSerialize(bytes : Array[Byte]) : Object ={
    try{
      val bais = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bais)
      val obj = ois.readObject()
      bais.close()
      ois.close()
      obj
    }catch{
      case e: Exception =>
        println("deSerialize" + e)
        null
    }
  }
}
