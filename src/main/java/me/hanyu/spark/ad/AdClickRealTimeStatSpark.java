package me.hanyu.spark.ad;

import java.util.ArrayList;
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
import me.hanyu.spark.dao.IAdUserClickCountDAO;
import me.hanyu.spark.dao.factory.DAOFactory;
import me.hanyu.spark.domain.AdBlacklist;
import me.hanyu.spark.domain.AdUserClickCount;
import me.hanyu.spark.util.DateUtils;
import scala.Tuple2;

public class AdClickRealTimeStatSpark {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStatSpark");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

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
		
		//filter based on dynamic black list
		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = 
				filterByBlacklist(adRealTimeLogDStream);
		
		generateDynamicBlacklist(filteredAdRealTimeLogDStream);

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
	
	
	private static JavaPairDStream<String, String> filterByBlacklist(
			JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
				new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>(){
					private static final long serialVersionUID = 1L;
					public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
						IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
						List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();
						
						List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();
						
						for(AdBlacklist adBlacklist : adBlacklists) {
							tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));  
						}
						
						JavaSparkContext sc = new JavaSparkContext(rdd.context());
						JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);
						//<userid, tuple2<string, string>>
						JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String,String>, Long, Tuple2<String, String>>() {
							private static final long serialVersionUID = 1L;
							public Tuple2<Long, Tuple2<String, String>> call(  
									Tuple2<String, String> tuple)
									throws Exception {
								String log = tuple._2;
								String[] logSplited = log.split(" "); 
								long userid = Long.valueOf(logSplited[3]);
								return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);  
							}
						});
						JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = 
								mappedRDD.leftOuterJoin(blacklistRDD);
						//keep users not in black list
						JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
								new Function<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, Boolean>() {
									private static final long serialVersionUID = 1L;
									public Boolean call(
											Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
											throws Exception {
										Optional<Boolean> optional = tuple._2._2;
										if(optional.isPresent() && optional.get()) {
											return false;
										}
										return true;
									}
								});
						JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
								new PairFunction<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, String, String>() {
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


	private static void generateDynamicBlacklist(
			JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
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
				
				JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(
						new Function<JavaRDD<Long>, JavaRDD<Long>>() {
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
								
								while(iterator.hasNext()) {
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

}
