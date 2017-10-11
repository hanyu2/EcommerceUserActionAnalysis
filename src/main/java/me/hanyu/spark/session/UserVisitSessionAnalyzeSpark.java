package me.hanyu.spark.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import me.hanyu.spark.conf.ConfigurationManager;
import me.hanyu.spark.constant.Constants;
import me.hanyu.spark.dao.ISessionAggrStatDAO;
import me.hanyu.spark.dao.ISessionDetailDAO;
import me.hanyu.spark.dao.ISessionRandomExtractDAO;
import me.hanyu.spark.dao.ITaskDAO;
import me.hanyu.spark.dao.ITop10CategoryDAO;
import me.hanyu.spark.dao.ITop10SessionDAO;
import me.hanyu.spark.dao.factory.DAOFactory;
import me.hanyu.spark.domain.SessionAggrStat;
import me.hanyu.spark.domain.SessionDetail;
import me.hanyu.spark.domain.SessionRandomExtract;
import me.hanyu.spark.domain.Task;
import me.hanyu.spark.domain.Top10Category;
import me.hanyu.spark.domain.Top10Session;
import me.hanyu.spark.util.DateUtils;
import me.hanyu.spark.util.NumberUtils;
import me.hanyu.spark.util.ParamUtils;
import me.hanyu.spark.util.SparkUtils;
import me.hanyu.spark.util.StringUtils;
import me.hanyu.spark.util.ValidUtils;
import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class UserVisitSessionAnalyzeSpark {
	public static void main(String[] args) {
		args = new String[] { "2" };

		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
				/*.set("spark.default.parallelism","100")*/
				.set("spark.storage.memoryFrachtion", "0.5")
				.set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")
				.set("spark.shuffle.consolidateFiles", "true")
				.set("spark.shuffle.file.buffer", "64")
				.set("spark.shuffle.memoryFraction", "0.3")
				.set("spark.reducer.maxSizeInFlight","24")
				.registerKryoClasses(new Class[]{CategorySortKey.class});
		SparkUtils.setMaster(conf);
		JavaSparkContext sc = new JavaSparkContext(conf);
		/*sc.checkpointFile("");*/
		SQLContext sqlContext = getSQLContext(sc.sc());

		SparkUtils.mockData(sc, sqlContext);

		ITaskDAO taskDAO = DAOFactory.getTaskDAO();

		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
		Task task = taskDAO.findById(taskid) ;
		if(task == null){
			System.out.println(new Date() + ":cannot find fask with id [" + taskid + "].");
		}
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		/*
		 * for(Row row : actionRDD.take(10)){
		 * System.out.println(row.toString()); }
		 */
		// <sessionid, action>
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
		sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
		/*sessionid2actionRDD = sessionid2actionRDD.checkpoint();*/

		// <sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sc, sqlContext, sessionid2actionRDD);
		// init our accumulator
		Accumulator<String> sessionAggStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD,
				taskParam, sessionAggStatAccumulator);
		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
		
		JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
				filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
		sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());

		randomExtractSession(sc, taskid, filteredSessionid2AggrInfoRDD, sessionid2detailRDD);

		calculateAndPersistAggrStat(sessionAggStatAccumulator.value(), task.getTaskid());
		List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(taskid, sessionid2detailRDD);
		getTop10Session(sc, taskid, top10CategoryList, sessionid2detailRDD);
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

	public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
		/*JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(2), row);
			}
		});*/
		return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

			private static final long serialVersionUID = 1L;

			public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
				List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
				while(iterator.hasNext()){
					Row row = iterator.next();
					list.add(new Tuple2<String, Row>(row.getString(2), row));//2 is session id
				}
				return list;
			}
		});
	}

	// aggregate by session id
	private static JavaPairRDD<String, String> aggregateBySession(JavaSparkContext sc, SQLContext sqlContext, JavaPairRDD<String, Row> sessionid2actionRDD) {
		
		// group action data by session
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2actionRDD.groupByKey();
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
						// session step length
						Date startTime = null;
						Date endTime = null;
						int stepLength = 0;
						while (iterator.hasNext()) {
							// rows are actions from a single sessionid
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
							// get session start end time
							Date actionTime = DateUtils.parseTime(row.getString(4));
							if (startTime == null) {
								startTime = actionTime;
							}
							if (endTime == null) {
								endTime = actionTime;
							}
							if (actionTime.before(startTime)) {
								startTime = actionTime;
							}
							if (actionTime.after(endTime)) {
								endTime = actionTime;
							}
							stepLength++;
						}
						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
						// session visit length
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "= " + searchKeywords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH
								+ "=" + stepLength + "|" + Constants.FIELD_START_TIME + "="
								+ DateUtils.formatTime(startTime);

						return new Tuple2<Long, String>(userid, partAggrInfo);
					}
				});
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		// <userid, userinfo>
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Row> call(Row row) throws Exception {
				return new Tuple2<Long, Row>(row.getLong(0), row);
			}
		});
		// <userid, <partAggrInfo(sessionid,searchKeywords,clickCategoryIds),
		// userinfo>>
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
		/*JavaPairRDD<String, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.mapToPair(new PairFunction<Tuple2<Long, Long>, String, Long>(){
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Long> call(Tuple2<Long, Long> tuple) throws Exception {
				Random random = new Random();
				int prefix = random.nextInt(10);
				return new Tuple2<String, Long>(prefix + "_" + tuple._1, tuple._2);
			}
		});
		
		//second do first reduce
		JavaPairRDD<String, Long> firstAggrRDD = clickCategoryId2CountRDD.reduceByKey(new Function2<Long, Long, Long>(){
			private static final long serialVersionUID = 1L;
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		});
		
		//third remove prefix from key
		JavaPairRDD<Long, Long> restoredRDD = firstAggrRDD.mapToPair(new PairFunction<Tuple2<String, Long>, Long, Long>(){
			private static final long serialVersionUID = 1L;
			public Tuple2<Long, Long> call(Tuple2<String, Long> tuple) throws Exception {
				long categoryId = Long.valueOf(tuple._1.split("_")[1]);
				return new Tuple2<Long, Long>(categoryId, tuple._2);
			}
		});
		//fourth full reduce
		JavaPairRDD<Long, Long> globalAggrRDD = restoredRDD.reduceByKey(
				new Function2<Long, Long, Long>(){
					private static final long serialVersionUID = 1L;
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
		});*/
		
		
		//sample and extend skewed keys
		/*JavaPairRDD<Long, String> sampledRDD = userid2PartAggrInfoRDD.sample(false, 0.1,9);
		JavaPairRDD<Long, Long> mappedSampledRDD = sampledRDD.mapToPair(
				
				new PairFunction<Tuple2<Long,String>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, Long> call(Tuple2<Long, String> tuple) throws Exception {
						return new Tuple2<Long,Long>(tuple._1,1L);
					}
				});
		JavaPairRDD<Long, Long> computedSampledRDD = mappedSampledRDD.reduceByKey(new Function2<Long, Long, Long>() {

			private static final long serialVersionUID = 1L;

			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		});
		
		JavaPairRDD<Long, Long> reversedSampledRDD = computedSampledRDD.mapToPair(
				
				new PairFunction<Tuple2<Long,Long>, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple) throws Exception {
						return new Tuple2<Long,Long>(tuple._2,tuple._1);
					}
				});
		
		final long skewedUserid = reversedSampledRDD.sortByKey(false).take(1).get(0)._2;
		
		JavaPairRDD<Long, String> skewedRDD = userid2PartAggrInfoRDD.filter(new Function<Tuple2<Long,String>, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<Long, String> tuple) throws Exception {
				return tuple._1.equals(skewedUserid);
			}
		});
		
		JavaPairRDD<Long, String> commonRDD = userid2PartAggrInfoRDD.filter(new Function<Tuple2<Long,String>, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<Long, String> tuple) throws Exception {
				return !tuple._1.equals(skewedUserid);
			}
		});
		
		JavaPairRDD<String, Row> skewedUserid2infoRDD = userid2InfoRDD.filter(
				
				new Function<Tuple2<Long,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<Long, Row> tuple) throws Exception {
						return tuple._1.equals(skewedUserid);
					}
		}).flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {

					private static final long serialVersionUID = 1L;
		
					public Iterable<Tuple2<String, Row>> call(
							Tuple2<Long, Row> tuple) throws Exception {
						Random random = new Random();
						
						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String,Row>>();
						
						for(int i = 0; i< 100; i++) {
							int prefix = random.nextInt(100);
							list.add(new Tuple2<String, Row>(prefix + "_" + tuple._1, tuple._2));
						}
						
						return list;
						
					}
				});
		
		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 = skewedRDD.mapToPair(
				
				new PairFunction<Tuple2<Long,String>, String, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
						Random random = new Random();
						int prefix = random.nextInt(100);
						return new Tuple2<String,String>(prefix + "_" + tuple._1,tuple._2);
					}
		}).join(skewedUserid2infoRDD)
		.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, Long, Tuple2<String,Row>>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Tuple2<String, Row>> call(
					Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				long userid = Long.valueOf(tuple._1.split("_")[1]);
				return new Tuple2<Long,Tuple2<String,Row>>(userid,tuple._2);
			}
		});
		
		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 = commonRDD.join(userid2InfoRDD);
		
		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD = joinedRDD1.union(joinedRDD2);
		
		JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						
						String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						
						String fullAggrInfo = partAggrInfo + "|"
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|" 
								+ Constants.FIELD_SEX + "=" + sex;
						
						return new Tuple2<String, String>(sessionid,fullAggrInfo);
					}
				});*/

		//extend skewed keys and join
	/*JavaPairRDD<String, Row> extendedRDD = userid2InfoRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
		
					private static final long serialVersionUID = 1L;
		
					public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String,Row>>();
						
						for(int i=0;i<10;i++) {
							list.add(new Tuple2<String,Row>(i + "_" + tuple._1,tuple._2));
						}
						
						return list;
					}
				});
		
		JavaPairRDD<String,String> mappedRDD = userid2PartAggrInfoRDD.mapToPair(
				
				new PairFunction<Tuple2<Long,String>, String, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
						Random random = new Random();
						int prefix = random.nextInt(10);
						return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
					}
		});
		
		JavaPairRDD<String, Tuple2<String,Row>> joinedRDD = mappedRDD.join(extendedRDD);
		
		JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(
				new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						
						String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						
						String fullAggrInfo = partAggrInfo + "|"
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|" 
								+ Constants.FIELD_SEX + "=" + sex;
						
						return new Tuple2<String, String>(sessionid,fullAggrInfo);
					}
				});*/
		return sessionid2FullAggrInfoRDD;
	}

	// filter session
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, final JSONObject taskParam,
			final Accumulator<String> sessionAggrStatAccumulator) {

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
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

		if (_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}

		final String parameter = _parameter;
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

				new Function<Tuple2<String, String>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, String> tuple) throws Exception {
						// get aggregated data
						String aggrInfo = tuple._2;

						// filter by age range
						if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE,
								Constants.PARAM_END_AGE)) {
							return false;
						}

						// filter by professionals
						if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter,
								Constants.PARAM_PROFESSIONALS)) {
							return false;
						}

						// filter by cities
						if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
							return false;
						}

						// filter by sex
						if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
							return false;
						}

						// filter by search keywords
						if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter,
								Constants.PARAM_KEYWORDS)) {
							return false;
						}

						// filter by category id
						if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter,
								Constants.PARAM_CATEGORY_IDS)) {
							return false;
						}
						// this is valid, increment session count
						sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
						// calculate visit length and step length
						long visitLength = Long.valueOf(
								StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
						long stepLength = Long.valueOf(
								StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
						calculateVisitLength(visitLength);
						calculateStepLength(stepLength);

						return true;
					}

					private void calculateVisitLength(long visitLength) {
						if (visitLength >= 1 && visitLength <= 3) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
						} else if (visitLength >= 4 && visitLength <= 6) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
						} else if (visitLength >= 7 && visitLength <= 9) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
						} else if (visitLength >= 10 && visitLength <= 30) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
						} else if (visitLength > 30 && visitLength <= 60) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
						} else if (visitLength > 60 && visitLength <= 180) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
						} else if (visitLength > 180 && visitLength <= 600) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
						} else if (visitLength > 600 && visitLength <= 1800) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
						} else if (visitLength > 1800) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
						}
					}

					private void calculateStepLength(long stepLength) {
						if (stepLength >= 1 && stepLength <= 3) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
						} else if (stepLength >= 4 && stepLength <= 6) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
						} else if (stepLength >= 7 && stepLength <= 9) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
						} else if (stepLength >= 10 && stepLength <= 30) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
						} else if (stepLength > 30 && stepLength <= 60) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
						} else if (stepLength > 60) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
						}
					}

				});

		return filteredSessionid2AggrInfoRDD;
	}

	private static JavaPairRDD<String, Row> getSessionid2detailRDD(
			JavaPairRDD<String, String> sessionid2aggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD
				.join(sessionid2actionRDD)
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
		
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Row> call(
							Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						return new Tuple2<String, Row>(tuple._1, tuple._2._2);
					}
					
				});
		return sessionid2detailRDD;
	}
	
	private static void randomExtractSession(
			JavaSparkContext sc,
			final long taskid, 
			JavaPairRDD<String, String> sessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		// <yyyy-MM--dd_HH, aggrinfo>
		JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
						String aggrInfo = tuple._2;
						String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
								Constants.FIELD_START_TIME);
						String dateHour = DateUtils.getDateHour(startTime);
						return new Tuple2<String, String>(dateHour, aggrInfo);
					}

				});
		Map<String, Object> countMap = time2sessionidRDD.countByKey();
		// Turn<yyyy-MM-dd_hh, count> => <yyyy_mm_dd, <hh, dd>>
		Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();

		for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
			String dateHour = countEntry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];

			long count = Long.valueOf(String.valueOf(countEntry.getValue()));

			Map<String, Long> hourCountMap = dateHourCountMap.get(date);
			if (hourCountMap == null) {
				hourCountMap = new HashMap<String, Long>();
				dateHourCountMap.put(date, hourCountMap);
			}

			hourCountMap.put(hour, count);
		}

		int extractNumberPerDay = Constants.SESSION_SAMPLE_SIZE / dateHourCountMap.size();

		// <date,<hour,(3,5,20,102)>>
		final Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();

		Random random = new Random();

		for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

			// count total session number from this day
			long sessionCount = 0L;
			for (long hourCount : hourCountMap.values()) {
				sessionCount += hourCount;
			}

			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if (hourExtractMap == null) {
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}

			// interate every hour
			for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();

				// number of extractions from this hour
				int hourExtractNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
				if (hourExtractNumber > count) {
					hourExtractNumber = (int) count;
				}

				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if (extractIndexList == null) {
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}

				for (int i = 0; i < hourExtractNumber; i++) {
					int extractIndex = random.nextInt((int) count);
					while (extractIndexList.contains(extractIndex)) {
						extractIndex = random.nextInt((int) count);
					}
					extractIndexList.add(extractIndex);
				}
			}
		}
		Map<String, Map<String,IntList>> fastutilDateHourExtractMap = 
				new HashMap<String, Map<String,IntList>>();
		for(Map.Entry<String, Map<String,List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()) {
			String date = dateHourExtractEntry.getKey();
			Map<String,List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
			Map<String,IntList> fastutilHourExtractMap = new HashMap<String, IntList>();
			
			for(Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
				String hour = hourExtractEntry.getKey();
				List<Integer> extractList = hourExtractEntry.getValue();
				IntList fastutilExtractList = new IntArrayList();
				
				for(int i = 0; i < extractList.size(); i++) {
					fastutilExtractList.add(extractList.get(i));
				}
				fastutilHourExtractMap.put(hour, fastutilExtractList);	
			}
			fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
		}
		
		final Broadcast<Map<String,Map<String,IntList>>> dateHourExtractMapBroadcast = 
				sc.broadcast(fastutilDateHourExtractMap);		
		// get< dateHour, (session, aggrinfo)>
		JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();
		JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(

				new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple)
							throws Exception {
						List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();

						String dateHour = tuple._1;
						String date = dateHour.split("_")[0];
						String hour = dateHour.split("_")[1];
						Iterator<String> iterator = tuple._2.iterator();
						
						Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast.value();
						
						List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
						
						ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

						int index = 0;
						while (iterator.hasNext()) {
							String sessionAggrInfo = iterator.next();

							if (extractIndexList.contains(index)) {
								String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|",
										Constants.FIELD_SESSION_ID);

								// insert into sql
								SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
								sessionRandomExtract.setTaskid(taskid);
								sessionRandomExtract.setSessionid(sessionid);
								sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo,
										"\\|", Constants.FIELD_START_TIME));
								sessionRandomExtract.setSearchKeyWords(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
								sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

								sessionRandomExtractDAO.insert(sessionRandomExtract);

								extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
							}

							index++;
						}

						return extractSessionids;
					}

				});

		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionidsRDD
				.join(sessionid2actionRDD);
		extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {

			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Row row = tuple._2._2;

				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskid);
				sessionDetail.setUserid(row.getLong(1));
				sessionDetail.setSessionid(row.getString(2));
				sessionDetail.setPageid(row.getLong(3));
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));
				sessionDetail.setClickCategoryId(row.getLong(6));
				sessionDetail.setClickProductId(row.getLong(7));
				sessionDetail.setOrderCategoryIds(row.getString(8));
				sessionDetail.setOrderProductIds(row.getString(9));
				sessionDetail.setPayCategoryIds(row.getString(10));
				sessionDetail.setPayProductIds(row.getString(11));

				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insert(sessionDetail);
			}
		});

	}

	private static void calculateAndPersistAggrStat(String value, Long taskid) {
		Long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

		long visit_length_1s_3s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
		long visit_length_4s_6s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

		long step_length_1_3 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

		double visit_length_1s_3s_ratio = NumberUtils.formatDouble((double) visit_length_1s_3s / (double) session_count,
				2);
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble((double) visit_length_4s_6s / (double) session_count,
				2);
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble((double) visit_length_7s_9s / (double) session_count,
				2);
		double visit_length_10s_30s_ratio = NumberUtils
				.formatDouble((double) visit_length_10s_30s / (double) session_count, 2);
		double visit_length_30s_60s_ratio = NumberUtils
				.formatDouble((double) visit_length_30s_60s / (double) session_count, 2);
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble((double) visit_length_1m_3m / (double) session_count,
				2);
		double visit_length_3m_10m_ratio = NumberUtils
				.formatDouble((double) visit_length_3m_10m / (double) session_count, 2);
		double visit_length_10m_30m_ratio = NumberUtils
				.formatDouble((double) visit_length_10m_30m / (double) session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble((double) visit_length_30m / (double) session_count, 2);

		double step_length_1_3_ratio = NumberUtils.formatDouble((double) step_length_1_3 / (double) session_count, 2);
		double step_length_4_6_ratio = NumberUtils.formatDouble((double) step_length_4_6 / (double) session_count, 2);
		double step_length_7_9_ratio = NumberUtils.formatDouble((double) step_length_7_9 / (double) session_count, 2);
		double step_length_10_30_ratio = NumberUtils.formatDouble((double) step_length_10_30 / (double) session_count,
				2);
		double step_length_30_60_ratio = NumberUtils.formatDouble((double) step_length_30_60 / (double) session_count,
				2);
		double step_length_60_ratio = NumberUtils.formatDouble((double) step_length_60 / (double) session_count, 2);

		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);
	}

	private static List<Tuple2<CategorySortKey, String>> getTop10Category(long taskid, JavaPairRDD<String, Row> sessionid2detailRDD) {
		// <categoryid, categoryid>
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(

				new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;

						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

						Long clickCategoryId = row.getLong(6);
						if (clickCategoryId != null) {
							list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
						}

						String orderCategoryIds = row.getString(8);
						if (orderCategoryIds != null) {
							String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
							for (String orderCategoryId : orderCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
										Long.valueOf(orderCategoryId)));
							}
						}

						String payCategoryIds = row.getString(10);
						if (payCategoryIds != null) {
							String[] payCategoryIdsSplited = payCategoryIds.split(",");
							for (String payCategoryId : payCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
										Long.valueOf(payCategoryId)));
							}
						}

						return list;
					}

				});
		categoryidRDD = categoryidRDD.distinct();
		// click counts of each category
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD);
		// order counts of each category
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD);
		// payment count of each category
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD);

		// category with its click, order, payment counts
		// leftOuterJoin
		JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD,
				orderCategoryId2CountRDD, payCategoryId2CountRDD);

		// define secondary sort key

		// <categorySortKey, info>
		JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(

				new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
						String countInfo = tuple._2;
						long clickCount = Long.valueOf(
								StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
						long orderCount = Long.valueOf(
								StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
						long payCount = Long.valueOf(
								StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));

						CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);

						return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
					}

				});
		//false means descending order
		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);
		
		//get top ten and store in mysql
		ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
		List<Tuple2<CategorySortKey, String>> top10CategoryList = 
				sortedCategoryCountRDD.take(10);
		
		for(Tuple2<CategorySortKey, String> tuple: top10CategoryList) {
			String countInfo = tuple._2;
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CATEGORY_ID));  
			long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CLICK_COUNT));  
			long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_ORDER_COUNT));  
			long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_PAY_COUNT));  
			
			Top10Category category = new Top10Category();
			category.setTaskid(taskid); 
			category.setCategoryid(categoryid); 
			category.setClickCount(clickCount);  
			category.setOrderCount(orderCount);
			category.setPayCount(payCount);
			
			top10CategoryDAO.insert(category);  
		}
		return top10CategoryList;
	}

	private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
		//filter with coalesce
		//local mode we don't need partition and parallelization numbers
		JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD
				.filter(new Function<Tuple2<String, Row>, Boolean>() {
					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return row.get(6) != null ? true : false;
					}
				})/*.coalesce(100)*/;

		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD
				.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
						long clickCategoryId = tuple._2.getLong(6);
						return new Tuple2<Long, Long>(clickCategoryId, 1L);
					}
				});
		
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD
				.reduceByKey(new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				}, 1000);//avoid data skew
		
		//double group
		//first add random to key
		/*JavaPairRDD<String, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.mapToPair(new PairFunction<Tuple2<Long, Long>, String, Long>(){
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Long> call(Tuple2<Long, Long> tuple) throws Exception {
				Random random = new Random();
				int prefix = random.nextInt(10);
				return new Tuple2<String, Long>(prefix + "_" + tuple._1, tuple._2);
			}
		});
		
		//second do first reduce
		JavaPairRDD<String, Long> firstAggrRDD = clickCategoryId2CountRDD.reduceByKey(new Function2<Long, Long, Long>(){
			private static final long serialVersionUID = 1L;
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		});
		
		//third remove prefix from key
		JavaPairRDD<Long, Long> restoredRDD = firstAggrRDD.mapToPair(new PairFunction<Tuple2<String, Long>, Long, Long>(){
			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Long> call(Tuple2<String, Long> tuple) throws Exception {
				long categoryId = Long.valueOf(tuple._1.split("_")[1]);
				return new Tuple2<Long, Long>(categoryId, tuple._2);
			}
		});
		//fouth full reduce
		JavaPairRDD<Long, Long> globalAggrRDD = restoredRDD.reduceByKey(
				new Function2<Long, Long, Long>(){
					private static final long serialVersionUID = 1L;
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
		});*/
		
		return clickCategoryId2CountRDD;
	}

	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(

				new Function<Tuple2<String, Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return row.getString(8) != null ? true : false;
					}

				});

		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(

				new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String orderCategoryIds = row.getString(8);
						String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

						for (String orderCategoryId : orderCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
						}

						return list;
					}

				});

		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD
				.reduceByKey(new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;

					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}

				});
		return orderCategoryId2CountRDD;
	}

	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(

				new Function<Tuple2<String, Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return row.getString(10) != null ? true : false;
					}

				});

		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(

				new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String payCategoryIds = row.getString(10);
						String[] payCategoryIdsSplited = payCategoryIds.split(",");

						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

						for (String payCategoryId : payCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
						}

						return list;
					}

				});

		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD
				.reduceByKey(new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;

					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}

				});

		return payCategoryId2CountRDD;
	}

	private static JavaPairRDD<Long, String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD, JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
		// leftOuterJoin means right might be null
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryidRDD
				.leftOuterJoin(clickCategoryId2CountRDD);
		JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(

				new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						Optional<Long> optional = tuple._2._2;
						long clickCount = 0L;

						if (optional.isPresent()) {
							clickCount = optional.get();
						}

						String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|"
								+ Constants.FIELD_CLICK_COUNT + "=" + clickCount;

						return new Tuple2<Long, String>(categoryid, value);
					}

				});
		tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(

				new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;

						Optional<Long> optional = tuple._2._2;
						long orderCount = 0L;

						if (optional.isPresent()) {
							orderCount = optional.get();
						}

						value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

						return new Tuple2<Long, String>(categoryid, value);
					}

				});

		tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(

				new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;

						Optional<Long> optional = tuple._2._2;
						long payCount = 0L;

						if (optional.isPresent()) {
							payCount = optional.get();
						}

						value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

						return new Tuple2<Long, String>(categoryid, value);
					}

				});

		return tmpMapRDD;
	}
	private static void getTop10Session(
			JavaSparkContext sc,
			final long taskid, 
			List<Tuple2<CategorySortKey, String>> top10CategoryList, 
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		List<Tuple2<Long, Long>> top10CategoryIdList = 
				new ArrayList<Tuple2<Long, Long>>();
		
		for(Tuple2<CategorySortKey, String> category : top10CategoryList) {
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));  
		}
		
		JavaPairRDD<Long, Long> top10CategoryIdRDD = 
				sc.parallelizePairs(top10CategoryIdList);
		JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD =
				sessionid2detailRDD.groupByKey();
		
		JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Long, String>> call(
							Tuple2<String, Iterable<Row>> tuple) throws Exception {
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
						
						// 计算出该session，对每个品类的点击次数
						while(iterator.hasNext()) {
							Row row = iterator.next();
							
							if(row.get(6) != null) {
								long categoryid = row.getLong(6);
								
								Long count = categoryCountMap.get(categoryid);
								if(count == null) {
									count = 0L;
								}
								
								count++;
								
								categoryCountMap.put(categoryid, count);
							}
						}
						
						// <categoryid,sessionid,count>
						List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
						
						for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
							long categoryid = categoryCountEntry.getKey();
							long count = categoryCountEntry.getValue();
							String value = sessionid + "," + count;
							list.add(new Tuple2<Long, String>(categoryid, value));  
						}
						
						return list;
					}
					
				}) ;
		
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
				categoryid2sessionCountRDD.groupByKey();
		
		JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<String, String>> call(
							Tuple2<Long, Iterable<String>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						Iterator<String> iterator = tuple._2.iterator();
						
						String[] top10Sessions = new String[Constants.TOP_SESSION_COUNT];  
						
						while(iterator.hasNext()) {
							String sessionCount = iterator.next();
							long count = Long.valueOf(sessionCount.split(",")[1]);  
							
							for(int i = 0; i < top10Sessions.length; i++) {
								if(top10Sessions[i] == null) {
									top10Sessions[i] = sessionCount;
									break;
								} else {
									long _count = Long.valueOf(top10Sessions[i].split(",")[1]);  
									
									if(count > _count) {
										for(int j = Constants.TOP_SESSION_COUNT - 1; j > i; j--) {
											top10Sessions[j] = top10Sessions[j - 1];
										}
										top10Sessions[i] = sessionCount;
										break;
									}
								}
							}
						}
						
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						
						for(String sessionCount : top10Sessions) {
							String sessionid = sessionCount.split(",")[0];
							long count = Long.valueOf(sessionCount.split(",")[1]);  
							
							Top10Session top10Session = new Top10Session();
							top10Session.setTaskid(taskid);  
							top10Session.setCategoryid(categoryid);  
							top10Session.setSessionid(sessionid);  
							top10Session.setClickCount(count);  
							
							ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
							top10SessionDAO.insert(top10Session);  
							
							list.add(new Tuple2<String, String>(sessionid, sessionid));
						}
						return list;
					}
				});
		
		//get top 10 session details and insert into mysql
		JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
				top10SessionRDD.join(sessionid2detailRDD);  
		sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {  
			private static final long serialVersionUID = 1L;
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Row row = tuple._2._2;
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskid);  
				sessionDetail.setUserid(row.getLong(1));  
				sessionDetail.setSessionid(row.getString(2));  
				sessionDetail.setPageid(row.getLong(3));  
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));  
				sessionDetail.setClickCategoryId(row.getLong(6));  
				sessionDetail.setClickProductId(row.getLong(7));   
				sessionDetail.setOrderCategoryIds(row.getString(8));  
				sessionDetail.setOrderProductIds(row.getString(9));  
				sessionDetail.setPayCategoryIds(row.getString(10)); 
				sessionDetail.setPayProductIds(row.getString(11));  
				
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insert(sessionDetail);  
			}
		});
		
	}

}
