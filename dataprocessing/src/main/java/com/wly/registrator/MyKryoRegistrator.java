package com.wly.registrator;

import com.atguigu.model.StartupReportLogs;
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

public class MyKryoRegistrator implements KryoRegistrator
{
  public void registerClasses(Kryo kryo)
  {
    kryo.register(StartupReportLogs.class);
  }
}
