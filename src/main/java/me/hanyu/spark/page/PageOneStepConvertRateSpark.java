package me.hanyu.spark.page;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;

import me.hanyu.spark.constant.Constants;
import me.hanyu.spark.dao.IPageSplitConvertRateDAO;
import me.hanyu.spark.dao.ITaskDAO;
import me.hanyu.spark.dao.factory.DAOFactory;
import me.hanyu.spark.domain.PageSplitConvertRate;
import me.hanyu.spark.domain.Task;
import me.hanyu.spark.util.DateUtils;
import me.hanyu.spark.util.NumberUtils;
import me.hanyu.spark.util.ParamUtils;
import me.hanyu.spark.util.SparkUtils;
import scala.Tuple2;

public class PageOneStepConvertRateSpark {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PAGE);
		SparkUtils.setMaster(conf);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		
		SparkUtils.mockData(sc, sqlContext);
		
		long taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);
		
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskid);
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
			return;
		}
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2actionRDD(actionRDD);
		JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();
		sessionid2actionsRDD = sessionid2actionsRDD.cache();
		JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(
				sc, sessionid2actionsRDD, taskParam);
		Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();
		long startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD);
		Map<String, Double> convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv);
		//save conversion rate into mysql
		persistConvertRate(taskid, convertRateMap);
	}
	private static JavaPairRDD<String, Row> getSessionid2actionRDD(
			JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Row> call(Row row) throws Exception {
				String sessionid = row.getString(2);
				return new Tuple2<String,Row>(sessionid,row);
			}
		});
	}
	private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
			JavaSparkContext sc,
			JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD,
			JSONObject taskParam) {
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final Broadcast<String> targetPageFlowBrocast = sc.broadcast(targetPageFlow);
		
		return sessionid2actionsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<String, Integer>> call(
							Tuple2<String, Iterable<Row>> tuple) throws Exception {
						List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String,Integer>>();
						Iterator<Row> iterator = tuple._2.iterator();
						
						String[] targetPages = targetPageFlowBrocast.value().split(",");
						
						
						//sort session actions based on time
						List<Row> rows = new ArrayList<Row>();
						while(iterator.hasNext()) {
							rows.add(iterator.next());
						}
						
						Collections.sort(rows, new Comparator<Row>() {

							public int compare(Row o1, Row o2) {
								String actionTime1 = o1.getString(4);
								String actionTime2 = o2.getString(4);
								
								Date date1 = DateUtils.parseTime(actionTime1);
								Date date2 = DateUtils.parseTime(actionTime2);
	
								return (int) (date1.getTime() - date2.getTime());
							}
						});
						
						Long lastPageId = null;
						
						for(Row row : rows) {
							Long pageid = row.getLong(3);
							
							//avoid first to be null
							if(lastPageId == null) {
								lastPageId = pageid;
								continue;
							}
							
							//generate a page split
							
							String pageSplit = lastPageId + "_" + pageid;
							
							//对这个切片判断一下，是否在用户指定的页面流中
							//Check whether user in this page slit
							for(int i = 1;i < targetPages.length;i++) {
								String targetPageSplit = targetPages[i-1] + "_" + targetPages[i];
								if(pageSplit.equals(targetPageSplit)) {
									list.add(new Tuple2<String,Integer>(pageSplit,1));
									break;
								}
							}
							lastPageId = pageid;
						}
						
						return list;
					}
		});
	}
	
	private static long  getStartPagePv(JSONObject taskParam,
			JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {
		//1,2,3,4,5,6,7,8,9
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		
		final long  startPageId = Long.valueOf(targetPageFlow.split(",")[0]);

		JavaRDD<Long> startPageRDD = sessionid2actionsRDD.flatMap(
				
				new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {
			
					private static final long serialVersionUID = 1L;
		
					public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
						List<Long> list = new ArrayList<Long>();
						
						Iterator<Row> iterator = tuple._2.iterator(); 
						
						while(iterator.hasNext()) {
							Row row = iterator.next();
							long pageid = row.getLong(3);//page_id
							
							if(pageid == startPageId) {
								list.add(pageid);
							}
						}
						return list;
					}
				});
		
		System.out.println(startPageRDD.count());
		return startPageRDD.count();
	}
	private static Map<String, Double> computePageSplitConvertRate(
			JSONObject taskParam,
			Map<String,Object> pageSplitPvMap,
			long startPagePv) {
		Map<String, Double> convertRateMap = new HashMap<String, Double>();
		
		String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");
		
		long lastPageSplitPv = 0L;
		
		for(int i = 1; i < targetPages.length;i++) {
			String targetPageSplit = targetPages[i-1] + "_" + targetPages[i];
			Long split  =  (Long) pageSplitPvMap.get(targetPageSplit);
			System.out.println(split);
			long targetPageSplitPv = split;
			
			double convertRate = 0.0;
			
			if(i == 1) {
				convertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv / (double)startPagePv, 2);
			} else {
				convertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv / (double)lastPageSplitPv, 2);
			}
			convertRateMap.put(targetPageSplit, convertRate);
			
			lastPageSplitPv = targetPageSplitPv;
		}
		
		return convertRateMap;
	}
	private static void persistConvertRate(Long taskid,
			Map<String, Double> convertRateMap) {
		StringBuffer buffer = new StringBuffer();
		
		for(Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
			String pageSplit = convertRateEntry.getKey();
			double convertRate = convertRateEntry.getValue();
			
			buffer.append(pageSplit + "=" + convertRate + "|");
		}
		
		String convertRate = buffer.toString();
		convertRate = convertRate.substring(0,convertRate.length()-1);
		
		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
		pageSplitConvertRate.setTaskid(taskid);
		pageSplitConvertRate.setConvertRate(convertRate);
		
		IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
		pageSplitConvertRateDAO.insert(pageSplitConvertRate);
		
	}
}
