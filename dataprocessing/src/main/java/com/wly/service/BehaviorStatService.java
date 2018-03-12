package com.wly.service;

import com.wly.common.HBaseClient;
import com.atguigu.model.UserBehaviorStatModel;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Properties;

public class BehaviorStatService
{
  private Properties props;
  private static BehaviorStatService service;

  public static BehaviorStatService getInstance(Properties props) {
    if (service == null) {
      synchronized (BehaviorStatService.class) {
        if (service == null) {
          service = new BehaviorStatService();
          service.props = props;
        }
      }
    }

    return service;
  }

  /*
  * 时长统计
  * */
  public void userNumberStat(UserBehaviorStatModel model) {
    addUserNumOfCity(model);
  }


  /*
  * 实时统计每个城市的用户数量
  * */
  public void addUserNumOfCity(UserBehaviorStatModel model) {
    String tableName = "online_city_users" ;
    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
    String rowKey = String.valueOf(model.getCity());

    try {
      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("StatisticData"), Bytes.toBytes("userNum"), 1L);
    } catch (Exception ex) {
      HBaseClient.closeTable(table);
      ex.printStackTrace();
    }
  }

}
