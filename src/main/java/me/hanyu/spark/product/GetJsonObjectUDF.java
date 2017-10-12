package me.hanyu.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import com.alibaba.fastjson.JSONObject;

//Json，Json field name，return value
public class GetJsonObjectUDF implements UDF2<String,String,String>{ 

	private static final long serialVersionUID = 1L;

	public String call(String json, String field) throws Exception {
		try {
			JSONObject jsonObject = JSONObject.parseObject(json);
			return jsonObject.getString(field);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
