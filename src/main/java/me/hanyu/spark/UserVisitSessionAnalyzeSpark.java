package me.hanyu.spark;

import java.util.Date;
import java.util.Iterator;

import org.apache.http.client.utils.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;

import me.hanyu.spark.conf.ConfigurationManager;
import me.hanyu.spark.constant.Constants;
import me.hanyu.spark.dao.ITaskDAO;
import me.hanyu.spark.dao.impl.DAOFactory;
import me.hanyu.spark.domain.Task;
import me.hanyu.spark.test.MockData;
import me.hanyu.spark.util.ParamUtils;
import me.hanyu.spark.util.StringUtils;
import me.hanyu.spark.util.ValidUtils;
import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class UserVisitSessionAnalyzeSpark {
	public static void main(String[] args) {
		args = new String[]{"2"};  
		
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
				.setMaster("local");    
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = getSQLContext(sc.sc());
		
		mockData(sc, sqlContext);
		
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		
		long taskid = ParamUtils.getTaskIdFromArgs(args);
		Task task = taskDAO.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
		
		// <sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sqlContext, actionRDD);
		System.out.println(sessionid2AggrInfoRDD.count());
		for(Tuple2<String, String> tuple : sessionid2AggrInfoRDD.take(10)){
			System.out.println(tuple._2);
		}
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = 
				filterSession(sessionid2AggrInfoRDD, taskParam);
		System.out.println(filteredSessionid2AggrInfoRDD.count());
		for(Tuple2<String, String> tuple : filteredSessionid2AggrInfoRDD.take(10)){
			System.out.println(tuple._2);
		}
		sc.close();
	}

	private static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}

	private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			MockData.mock(sc, sqlContext);  
		}
	}

	private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		String sql = "select * " + "from user_visit_action " + "where date>='" + startDate + "' " + "and date<='"
				+ endDate + "'";

		DataFrame actionDF = sqlContext.sql(sql);

		return actionDF.javaRDD();
	}

	// aggregate by session id
	private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {
		JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
			private static final long serialVersionUID = 1L;

			// aggregate by sessionid
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(2), row);
			}
		});
		// group action data by session
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();
		// aggregate by search word and product category
		// <userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
						// TODO Auto-generated method stub
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
						Long userid = null;
						//session step length
						Date startTime = null;
						Date endTime = null;
						int stepLength = 0;
						while (iterator.hasNext()) {
							Row row = iterator.next();
							if (userid == null) {
								userid = row.getLong(1);
							}
							String searchKeyword = row.getString(5);
							Long clickCategoryId = row.getLong(6);
							
							if (StringUtils.isNotEmpty(searchKeyword)) {
								if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
									searchKeywordsBuffer.append(searchKeyword + ",");
								}
							}
							if (clickCategoryId != null) {
								if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
									clickCategoryIdsBuffer.append(clickCategoryId + ",");
								}
							}
							//get session start end time
							Date actionTime = DateUtils.parseDate(row.getString(4));
							if(startTime == null) {
								startTime = actionTime;
							}
							if(endTime == null) {
								endTime = actionTime;
							}
							if(actionTime.before(startTime)) {
								startTime = actionTime;
							}
							if(actionTime.after(endTime)) {
								endTime = actionTime;
							}
							stepLength++;
						}
						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
						//session visit length
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid +"|" 
								+ Constants.FIELD_SEARCH_KEYWORDS + "= " + searchKeywords + "|" 
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" 
								+ Constants.FIELD_STEP_LENGTH + "=" + stepLength;

						return new Tuple2<Long, String>(userid, partAggrInfo);
					}
				});
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Row> call(Row row) throws Exception {
				return new Tuple2<Long, Row>(row.getLong(0), row);
			}
		});

		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|",
								Constants.FIELD_SESSION_ID);
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);

						String fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "="
								+ city + "|" + Constants.FIELD_SEX + "=" + sex;

						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
				});

		return sessionid2FullAggrInfoRDD;
	}

	// filter session
	private static JavaPairRDD<String, String> filterSession(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam) {
		
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
		
		if(_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		
		final String parameter = _parameter;
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
				
				new Function<Tuple2<String,String>, Boolean>() {
			
					private static final long serialVersionUID = 1L;
			
					public Boolean call(Tuple2<String, String> tuple) throws Exception {
						// get aggregated data
						String aggrInfo = tuple._2;
						
						//filter by age range
						if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, 
								parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
							return false;
						}
						
						//filter by professionals
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, 
								parameter, Constants.PARAM_PROFESSIONALS)) {
							return false;
						}
						
						//filter by cities
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, 
								parameter, Constants.PARAM_CITIES)) {
							return false;
						}
						
						//filter by sex
						if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, 
								parameter, Constants.PARAM_SEX)) {
							return false;
						}
						
						//filter by search keywords
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, 
								parameter, Constants.PARAM_KEYWORDS)) {
							return false;
						}
						
						//filter by category id
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, 
								parameter, Constants.PARAM_CATEGORY_IDS)) {
							return false;
						}
						return true;
					}
				});
		
		return filteredSessionid2AggrInfoRDD;
	}

}
