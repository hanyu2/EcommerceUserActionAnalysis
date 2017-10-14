package me.hanyu.spark.ad;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import kafka.serializer.StringDecoder;
import me.hanyu.spark.conf.ConfigurationManager;
import me.hanyu.spark.constant.Constants;
import me.hanyu.spark.dao.IAdBlacklistDAO;
import me.hanyu.spark.dao.IAdClickTrendDAO;
import me.hanyu.spark.dao.IAdProvinceTop3DAO;
import me.hanyu.spark.dao.IAdStatDAO;
import me.hanyu.spark.dao.IAdUserClickCountDAO;
import me.hanyu.spark.dao.factory.DAOFactory;
import me.hanyu.spark.domain.AdBlacklist;
import me.hanyu.spark.domain.AdClickTrend;
import me.hanyu.spark.domain.AdProvinceTop3;
import me.hanyu.spark.domain.AdStat;
import me.hanyu.spark.domain.AdUserClickCount;
import me.hanyu.spark.util.DateUtils;
import scala.Tuple2;

public class AdClickRealTimeStatSpark {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStatSpark");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		jssc.checkpoint("hdfs://master:9090/checkpoint");
		

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

		String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
		String[] kafkaTopicsSplited = kafkaTopics.split(",");

		Set<String> topics = new HashSet<String>();
		for (String kafkaTopic : kafkaTopicsSplited) {
			topics.add(kafkaTopic);
		}

		// kafka direct api
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		//adRealTimeLogDStream.repartition(1000);

		// filter based on dynamic black list
		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream);

		generateDynamicBlacklist(filteredAdRealTimeLogDStream);

		// get each province, each city ads click counts
		// update mysql continuously
		// (yyyyMMdd_province_city_adid,clickCount）
		JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream);
		
		calculateProvinceTop3Ad(adRealTimeStatDStream);
		calculateAdClickCountByWindow(adRealTimeLogDStream);
		

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}




	private static JavaPairDStream<String, String> filterByBlacklist(
			JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream
				.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
					private static final long serialVersionUID = 1L;

					public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
						IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
						List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();

						List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();

						for (AdBlacklist adBlacklist : adBlacklists) {
							tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));
						}

						JavaSparkContext sc = new JavaSparkContext(rdd.context());
						JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);
						// <userid, tuple2<string, string>>
						JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd
								.mapToPair(new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
									private static final long serialVersionUID = 1L;

									public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple)
											throws Exception {
										String log = tuple._2;
										String[] logSplited = log.split(" ");
										long userid = Long.valueOf(logSplited[3]);
										return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
									}
								});
						JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD
								.leftOuterJoin(blacklistRDD);
						// keep users not in black list
						JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD
								.filter(new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
									private static final long serialVersionUID = 1L;

									public Boolean call(
											Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
											throws Exception {
										Optional<Boolean> optional = tuple._2._2;
										if (optional.isPresent() && optional.get()) {
											return false;
										}
										return true;
									}
								});
						JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
								new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
									private static final long serialVersionUID = 1L;

									public Tuple2<String, String> call(
											Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
											throws Exception {
										return tuple._2._1;
									}

								});
						return resultRDD;
					}
				});
		return filteredAdRealTimeLogDStream;
	}

	private static void generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
		// rows of real time log
		// format log to <yyyyMMdd_userid_adid, 1L>
		// in each batch
		JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
						// get log
						String log = tuple._2;
						String[] logSplited = log.split(" ");

						String timestamp = logSplited[0];
						Date date = new Date(Long.valueOf(timestamp));
						String datekey = DateUtils.formatDateKey(date);

						long userid = Long.valueOf(logSplited[3]);
						long adid = Long.valueOf(logSplited[4]);

						// concat key
						String key = datekey + "_" + userid + "_" + adid;

						return new Tuple2<String, Long>(key, 1L);
					}

				});

		// <yyyyMMdd_userid_adid, clickCount>
		JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream
				.reduceByKey(new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;

					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});

		dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
			private static final long serialVersionUID = 1L;

			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
					private static final long serialVersionUID = 1L;

					public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
						List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();
						while (iterator.hasNext()) {
							Tuple2<String, Long> tuple = iterator.next();

							String[] keySplited = tuple._1.split("_");
							String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
							// yyyy-MM-dd
							long userid = Long.valueOf(keySplited[1]);
							long adid = Long.valueOf(keySplited[2]);
							long clickCount = tuple._2;

							AdUserClickCount adUserClickCount = new AdUserClickCount();
							adUserClickCount.setDate(date);
							adUserClickCount.setUserid(userid);
							adUserClickCount.setAdid(adid);
							adUserClickCount.setClickCount(clickCount);

							adUserClickCounts.add(adUserClickCount);
						}
						IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
						adUserClickCountDAO.updateBatch(adUserClickCounts);
					}
				});
				return null;
			}
		});

		JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(

				new Function<Tuple2<String, Long>, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Long> tuple) throws Exception {
						String key = tuple._1;
						String[] keySplited = key.split("_");

						// yyyyMMdd -> yyyy-MM-dd
						String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
						long userid = Long.valueOf(keySplited[1]);
						long adid = Long.valueOf(keySplited[2]);

						IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
						int clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userid, adid);

						if (clickCount >= 100) {
							return true;
						}

						return false;
					}

				});
		JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(new Function<Tuple2<String, Long>, Long>() {
			private static final long serialVersionUID = 1L;

			public Long call(Tuple2<String, Long> tuple) throws Exception {
				String key = tuple._1;
				String[] keySplited = key.split("_");
				Long userid = Long.valueOf(keySplited[1]);
				return userid;
			}
		});

		JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream
				.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {
					private static final long serialVersionUID = 1L;

					public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
						return rdd.distinct();
					}
				});
		distinctBlacklistUseridDStream.foreachRDD(new Function<JavaRDD<Long>, Void>() {

			private static final long serialVersionUID = 1L;

			public Void call(JavaRDD<Long> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
					private static final long serialVersionUID = 1L;

					public void call(Iterator<Long> iterator) throws Exception {
						List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();

						while (iterator.hasNext()) {
							long userid = iterator.next();

							AdBlacklist adBlacklist = new AdBlacklist();
							adBlacklist.setUserid(userid);

							adBlacklists.add(adBlacklist);
						}

						IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
						adBlacklistDAO.insertBatch(adBlacklists);
					}
				});
				return null;
			}
		});
	}

	private static JavaPairDStream<String, Long> calculateRealTimeStat(
			JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
		JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(
				new PairFunction<Tuple2<String, String>, String, Long>() {
					private static final long serialVersionUID = 1L;
					public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
						String log = tuple._2;
						String[] logSplited = log.split(" ");

						String timestamp = logSplited[0];
						Date date = new Date(Long.valueOf(timestamp));
						String datekey = DateUtils.formatDateKey(date); // yyyyMMdd

						String province = logSplited[1];
						String city = logSplited[2];
						long adid = Long.valueOf(logSplited[4]);
						String key = datekey + "_" + province + "_" + city + "_" + adid;
						return new Tuple2<String, Long>(key, 1L);
					}
				});
		//in each key, add up each key's value
		JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey(
				
				new Function2<List<Long>, Optional<Long>, Optional<Long>>() {

					private static final long serialVersionUID = 1L;

					public Optional<Long> call(List<Long> values, Optional<Long> optional)
							throws Exception {
						
						long clickCount = 0L;
						
						if(optional.isPresent()) {
							clickCount = optional.get();
						}
						
						//in each batch, key's value
						for(Long value : values) {
							clickCount += value;
						}
						
						return Optional.of(clickCount);  
					}
					
				});
		aggregatedDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {

			private static final long serialVersionUID = 1L;

			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {

					private static final long serialVersionUID = 1L;

					public void call(Iterator<Tuple2<String, Long>> iterator)
							throws Exception {
						List<AdStat> adStats = new ArrayList<AdStat>();
					
						while(iterator.hasNext()) {
							Tuple2<String, Long> tuple = iterator.next();
							
							String[] keySplited = tuple._1.split("_");
							String date = keySplited[0];
							String province = keySplited[1];
							String city = keySplited[2];
							long adid = Long.valueOf(keySplited[3]);  
							
							long clickCount = tuple._2;
							
							AdStat adStat = new AdStat();
							adStat.setDate(date); 
							adStat.setProvince(province);  
							adStat.setCity(city);  
							adStat.setAdid(adid); 
							adStat.setClickCount(clickCount);  
							
							adStats.add(adStat);
						}
						
						IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
						adStatDAO.updateBatch(adStats);  
					}
					
				});
				
				return null;
			}
			
		});
		
		return aggregatedDStream;
	}
	

	private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
		JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(
				
				new Function<JavaPairRDD<String,Long>, JavaRDD<Row>>() {

					private static final long serialVersionUID = 1L;

					
					public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd)
							throws Exception {
						
						// <yyyyMMdd_province_city_adid, clickCount>
						// <yyyyMMdd_province_adid, clickCount>
						JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(
								
								new PairFunction<Tuple2<String,Long>, String, Long>() {

									private static final long serialVersionUID = 1L;
		
									
									public Tuple2<String, Long> call(
											Tuple2<String, Long> tuple) throws Exception {
										String[] keySplited = tuple._1.split("_");
										String date = keySplited[0];
										String province = keySplited[1];
										long adid = Long.valueOf(keySplited[3]);
										long clickCount = tuple._2;
										
										String key = date + "_" + province + "_" + adid;
										
										return new Tuple2<String, Long>(key, clickCount);   
									}
									
								});
						
						JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(
								
								new Function2<Long, Long, Long>() {

									private static final long serialVersionUID = 1L;

									
									public Long call(Long v1, Long v2)
											throws Exception {
										return v1 + v2;
									}
									
								});
						
						// dailyAdClickCountByProvinceRDD=>DataFrame
						JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(
								
								new Function<Tuple2<String,Long>, Row>() {

									private static final long serialVersionUID = 1L;

									
									public Row call(Tuple2<String, Long> tuple)
											throws Exception {
										String[] keySplited = tuple._1.split("_");  
										String datekey = keySplited[0];
										String province = keySplited[1];
										long adid = Long.valueOf(keySplited[2]);  
										long clickCount = tuple._2;
										
										String date = DateUtils.formatDate(DateUtils.parseDateKey(datekey));  
										
										return RowFactory.create(date, province, adid, clickCount);  
									}
									
								});
						
						StructType schema = DataTypes.createStructType(Arrays.asList(
								DataTypes.createStructField("date", DataTypes.StringType, true),
								DataTypes.createStructField("province", DataTypes.StringType, true),
								DataTypes.createStructField("ad_id", DataTypes.LongType, true),
								DataTypes.createStructField("click_count", DataTypes.LongType, true)));  
						
						HiveContext sqlContext = new HiveContext(rdd.context());
						
						DataFrame dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowsRDD, schema);
						
						dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");  
						
						DataFrame provinceTop3AdDF = sqlContext.sql(
								"SELECT "
									+ "date,"
									+ "province,"
									+ "ad_id,"
									+ "click_count "
								+ "FROM ( "
									+ "SELECT "
										+ "date,"
										+ "province,"
										+ "ad_id,"
										+ "click_count,"
										+ "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank "
									+ "FROM tmp_daily_ad_click_count_by_prov "
								+ ") t "
								+ "WHERE rank>=3"
						);  
						
						return provinceTop3AdDF.javaRDD();
					}
					
				});
		
		// rowsDStream
		// top 3 ads of each province每次都是刷新出来各个省份最热门的top3广告
		// save into mysql
		rowsDStream.foreachRDD(new Function<JavaRDD<Row>, Void>() {
			
			private static final long serialVersionUID = 1L;

			public Void call(JavaRDD<Row> rdd) throws Exception {
				
				rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {

					private static final long serialVersionUID = 1L;

					public void call(Iterator<Row> iterator) throws Exception {
						List<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();
						
						while(iterator.hasNext()) {
							Row row = iterator.next();
							String date = row.getString(0);
							String province = row.getString(1);
							long adid = row.getLong(2);
							long clickCount = row.getLong(3);
							
							AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
							adProvinceTop3.setDate(date); 
							adProvinceTop3.setProvince(province); 
							adProvinceTop3.setAdid(adid);  
							adProvinceTop3.setClickCount(clickCount); 
							
							adProvinceTop3s.add(adProvinceTop3);
						}
						IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
						adProvinceTop3DAO.updateBatch(adProvinceTop3s);  
					}
				});
				return null;
			}
		});
	}
	

	private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		//<yyyyMMddHHMM_adid,1L>
		JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(
				new PairFunction<Tuple2<String,String>, String, Long>() {
					private static final long serialVersionUID = 1L;
					public Tuple2<String, Long> call(Tuple2<String, String> tuple)
							throws Exception {
						// timestamp province city userid adid
						String[] logSplited = tuple._2.split(" ");  
						String timeMinute = DateUtils.formatTimeMinute(
								new Date(Long.valueOf(logSplited[0])));  
						long adid = Long.valueOf(logSplited[4]);  
						return new Tuple2<String, Long>(timeMinute + "_" + adid, 1L);  
					}
				});
		JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(
				new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				}, Durations.minutes(60), Durations.seconds(10));
		
		aggrRDD.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {

			private static final long serialVersionUID = 1L;

			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {

					private static final long serialVersionUID = 1L;

					public void call(Iterator<Tuple2<String, Long>> iterator)
							throws Exception {
						List<AdClickTrend> adClickTrends = new ArrayList<AdClickTrend>();
						
						while(iterator.hasNext()) {
							Tuple2<String, Long> tuple = iterator.next();
							String[] keySplited = tuple._1.split("_"); 
							// yyyyMMddHHmm
							String dateMinute = keySplited[0];
							long adid = Long.valueOf(keySplited[1]);
							long clickCount = tuple._2;
							
							String date = DateUtils.formatDate(DateUtils.parseDateKey(
									dateMinute.substring(0, 8)));  
							String hour = dateMinute.substring(8, 10);
							String minute = dateMinute.substring(10);
							
							AdClickTrend adClickTrend = new AdClickTrend();
							adClickTrend.setDate(date); 
							adClickTrend.setHour(hour);  
							adClickTrend.setMinute(minute);  
							adClickTrend.setAdid(adid);  
							adClickTrend.setClickCount(clickCount);  
							
							adClickTrends.add(adClickTrend);
						}
						
						IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
						adClickTrendDAO.updateBatch(adClickTrends);
					}
				});
				return null;
			}
		});
		
	}

}
