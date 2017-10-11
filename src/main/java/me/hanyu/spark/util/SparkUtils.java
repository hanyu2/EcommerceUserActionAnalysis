package me.hanyu.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;

import me.hanyu.spark.conf.ConfigurationManager;
import me.hanyu.spark.constant.Constants;
import me.hanyu.spark.test.MockData;

public class SparkUtils {
	public static void setMaster(SparkConf conf) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			conf.setMaster("local");
		}
	}
	
	public static void mockData(JavaSparkContext sc,SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			MockData.mock(sc, sqlContext);
		}
	}
	
	public static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
	
	public static JavaRDD<Row> getActionRDDByDateRange(
			SQLContext sqlContext,JSONObject taskParam) {
		System.out.println("hanshulimian :" + taskParam);
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
	
		String sql = "select * "
				+ "from user_visit_action "
				+ "where date >='" + startDate + "' "
				+ "and date <='" + endDate + "'"; 
		
		DataFrame actionDF = sqlContext.sql(sql);
	
		/**
		 * 这里就可能发生问题
		 * 比如说，Spark SQL默认会给第一个stage设置了20个task，但是根据你的数据量以及
		 * 算法的复杂度，实际上，你需要1000个task去并行执行
		 * 
		 * 所以说，这里就可以对spark sql查询出来的RDD执行repartition重分区操作
		 */
		//return actionDF.javaRDD().repartition(100);
		
		return actionDF.javaRDD();
	}
}
