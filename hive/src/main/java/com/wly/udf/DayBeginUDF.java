package com.wly.udf;

import com.wly.util.DateUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DayBeginUDF extends UDF {
    // 计算现在的起始时刻(毫秒数)
    public long evaluate(){
        return evaluate(new Date());
    }

    // 计算某天的起始时刻，日期类型(毫秒数)
    public long evaluate(Date date) {
        return DateUtil.getDayBeginTime(date).getTime();
    }


    // 指定天偏移量
    public long evaluate(int offset){
        return evaluate(new Date(), offset);
    }

    // 计算某天的起始时刻，日期类型，带偏移量(毫秒数)
    public long evaluate(Date date, int offset) {
        return DateUtil.getDayBeginTime(date, offset).getTime();
    }

    // 计算某天的起始时刻，String类型(毫秒数)
    public long evaluate(String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date d = sdf.parse(dateStr);
        return evaluate(d);
    }

    // 计算某天的起始时刻，String类型，带偏移量(毫秒数)
    public long evaluate(String dateStr,int offset) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date d = sdf.parse(dateStr);
        return evaluate(d, offset);
    }

    // 计算某天的起始时刻，String类型，带格式化要求(毫秒数)
    public long evaluate(String dateStr,String fmt) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date date = sdf.parse(dateStr);
        return evaluate(date);
    }

    // 计算某天的起始时刻，String类型，带格式化，带偏移量(毫秒数)
    public long evaluate(String dateStr,String fmt,int offset) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date d = sdf.parse(dateStr);
        return evaluate(d, offset);
    }
}
