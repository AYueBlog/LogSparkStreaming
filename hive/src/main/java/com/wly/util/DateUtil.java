package com.wly.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 工具类实现
 */
public class DateUtil {

    /**
     * 得到指定date的零时刻.
     */
    public static Date getDayBeginTime(Date d) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
        try {
            return sdf.parse(sdf.format(d));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 得到指定date的偏移量零时刻.
     */
    public static Date getDayBeginTime(Date d,int offset) {
        try {
            SimpleDateFormat sdf =new SimpleDateFormat("yyyy/MM/dd 00:00:00");
            Date beginDate = sdf.parse(sdf.format(d));
            Calendar c = Calendar.getInstance();
            c.setTime(beginDate);
            c.add(Calendar.DAY_OF_MONTH,offset);
            return c.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定date所在周的起始时刻.
     */
    public static Date getWeekBeginTime(Date d) {
        Date dayBeginTime = getDayBeginTime(d);
        Calendar c = Calendar.getInstance();
        c.setTime(dayBeginTime);
        int n = c.get(Calendar.DAY_OF_WEEK);
        c.add(Calendar.DAY_OF_MONTH,-(n-1));
        return c.getTime();
    }

    /**
     * 得到指定date所在偏移周的起始时刻.
     */
    public static Date getWeekBeginTime(Date d,int offset) {
        Date dayBeginTime = getWeekBeginTime(d);
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        c.add(Calendar.WEEK_OF_MONTH,offset);
        return c.getTime();
    }


    /**
     * 得到指定date所在月的起始时刻.
     */
    public static Date getMonthBeginTime(Date d) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");
            return sdf.parse(sdf.format(d));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 得到指定date所在偏移月的起始时刻.
     */
    public static Date getMonthBeginTime(Date d,int offset){
        Date monthBeginTime = getMonthBeginTime(d);
        Calendar c = Calendar.getInstance();
        c.setTime(monthBeginTime);
        c.add(Calendar.MONTH,offset);
        return c.getTime();
    }

}
