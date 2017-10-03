package me.hanyu.spark.conf;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
	private static Properties props = new Properties();
	static{
		try {
			InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
			props.load(in);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	public static String getProperty(String key){
		return props.getProperty(key);
	}
	public static Integer getInteger(String key){
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return 0;
	}
	public static Boolean getBoolean(String key){
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return false;
	}
}
