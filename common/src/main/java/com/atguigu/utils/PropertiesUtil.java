package com.atguigu.utils;

import java.io.*;
import java.util.Properties;

public final class PropertiesUtil
{
  private PropertiesUtil()
  {
  }
  public static Properties props = null;

  public static Properties getProperties(String path) throws IOException
  {

    InputStream in = null;

    try {
      in = new BufferedInputStream(ClassLoader.getSystemResourceAsStream(path));
      props = new Properties();
      props.load(in);
      return props;
    } catch (IOException e) {
      throw e;
    } finally {
      if (in != null) {
        in.close();
      }
    }

  }
//  static{
//    try {
//      InputStream is = ClassLoader.getSystemResourceAsStream("hdfs_consumer.properties");
//      props = new Properties();
//      props.load(is);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
//
//
//  public static String getProperty(String key){
//    return props.getProperty(key);
//  }
}
