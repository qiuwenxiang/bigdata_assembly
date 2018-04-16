package com.kylin.assembly.strom;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


/**
 * @Description	TODO
 * @ClassName	DateHelper
 * @Date		2016年5月18日 下午5:59:35
 * @Author		qiuwenxiang@tydic.com
 * Copyright (c) All Rights Reserved, 2016.
 */

public class DateHelper {
	/**
	 * yyyy-MM-dd HH:mm:ss 类型
	 */
	public static String format1="yyyy-MM-dd HH:mm:ss";
	
	/**
	 * yyyyMMddHHmmss 类型
	 */
	public static String format2="yyyyMMddHHmmss";
	/**
	 * 
	 * @Description 得到当前时间
	 * @param @return 参数
	 * @return String 返回类型
	 */
	public static String getDate(String format){
		SimpleDateFormat df = new SimpleDateFormat(format);
		return df.format(new Date());
	}
	
	/**
	 * 
	 * @Description 得到星期
	 * @param @return 参数
	 * @return String 返回类型
	 */
	public static int getWeek(){
		return new Date().getDay();
	}
	
	/**
	 * 
	 * @Description 得到 距离现在多少天的 时间 ,传入时间类型
	 * @param 参数
	 * @return String 返回类型
	 */
	public static String getDate(String format,int amount){
		 Date date=new Date();//取时间
		 Calendar calendar = new GregorianCalendar();
		 calendar.setTime(date);
		 calendar.add(calendar.DATE,amount);//把日期往后增加一天.整数往后推,负数往前移动
		 date=calendar.getTime(); //这个时间就是日期往后推一天的结果 
		 SimpleDateFormat formatter = new SimpleDateFormat(format);
		 return formatter.format(date);
	}
	  //yyyyMMddHHmmss  转为yyyyMM
    public static String formatDate(String time) {
 		String reTime = "";
 		try {
 			reTime = new SimpleDateFormat("yyyyMM").format(new SimpleDateFormat("yyyyMMddHHmmss").parse(time));
 		} catch (ParseException e) {
 		}
 		return reTime;
 	}
    
}
