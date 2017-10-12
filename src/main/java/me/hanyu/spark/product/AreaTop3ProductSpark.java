package me.hanyu.spark.product;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;

import me.hanyu.spark.conf.ConfigurationManager;
import me.hanyu.spark.constant.Constants;
import me.hanyu.spark.dao.IAreaTop3ProductDAO;
import me.hanyu.spark.dao.ITaskDAO;
import me.hanyu.spark.dao.factory.DAOFactory;
import me.hanyu.spark.domain.AreaTop3Product;
import me.hanyu.spark.domain.Task;
import me.hanyu.spark.util.ParamUtils;
import me.hanyu.spark.util.SparkUtils;
import scala.Tuple2;

public class AreaTop3ProductSpark {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("AreaTop3ProductSpark");
		SparkUtils.setMaster(conf);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		
		sqlContext.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
		sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());
		sqlContext.udf().register("get_json_object", new GetJsonObjectUDF(),DataTypes.StringType);
		sqlContext.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
		
		SparkUtils.mockData(sc, sqlContext);

		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
		Task task = taskDAO.findById(taskid);

		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		// <cityId, action>
		JavaPairRDD<Long, Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(sqlContext, startDate, endDate);
		//<cityId, info>
		JavaPairRDD<Long,Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);
		generateTempClickProductBasicTable(sqlContext, cityid2clickActionRDD, cityid2cityInfoRDD);
		generateTempAreaProductClickCountTable(sqlContext);
		generateTempAreaProductFullProductClickCountTable(sqlContext);
		JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);
		List<Row> rows = areaTop3ProductRDD.collect();
		persistAreaTop3Product(taskid, rows);
		sc.close();
	}



	private static JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(SQLContext sqlContext, String startDate,
			String endDate) {
		String sql = "SELECT " + "city_id," + "click_product_id product_id " + "FROM user_visit_action "
				+ "WHERE click_product_id IS NOT NULL " + "AND date >= '" + startDate + "' " + "AND date <= '" + endDate
				+ "'";
		DataFrame clickActionDF = sqlContext.sql(sql);
		JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();
		JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Long, Row> call(Row row) throws Exception {
				Long cityid = row.getLong(0);
				return new Tuple2<Long, Row>(cityid, row);
			}

		});
		return cityid2clickActionRDD;
	}

	private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
		String url = null;
		String user = null;
		String password = null;
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);

		} else {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
		}

		Map<String, String> options = new HashMap<String, String>();
		options.put("url", url);
		options.put("dbtable", "city_info");
		options.put("user", user);
		options.put("password", password);

		DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();
		JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
		JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(

				new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Long, Row> call(Row row) throws Exception {
						long cityid = Long.valueOf(String.valueOf(row.get(0)));
						return new Tuple2<Long, Row>(cityid, row);
					}
				});

		return cityid2cityInfoRDD;
	}
	
	private static void generateTempClickProductBasicTable(
			SQLContext sqlContext,
			JavaPairRDD<Long, Row> cityid2clickActionRDD,
			JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
		// 执行join操作，进行点击行为数据和城市数据的关联
		JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD =
				cityid2clickActionRDD.join(cityid2cityInfoRDD);
		
		// 将上面的JavaPairRDD，转换成一个JavaRDD<Row>（才能将RDD转换为DataFrame）
		JavaRDD<Row> mappedRDD = joinedRDD.map(
				
				new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {

					private static final long serialVersionUID = 1L;

					public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple)
							throws Exception {
						long cityid = tuple._1;
						Row clickAction = tuple._2._1;
						Row cityInfo = tuple._2._2;
						
						long productid = clickAction.getLong(1);
						String cityName = cityInfo.getString(1);
						String area = cityInfo.getString(2);
						
						return RowFactory.create(cityid, cityName, area, productid);  
					}
					
				});
		
		// JavaRDD<Row>=>DataFrame
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));  
		structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));  
		
		StructType schema = DataTypes.createStructType(structFields);
	
		DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);
		
		df.registerTempTable("tmp_clk_prod_basic");  
	}
	
	private static void generateTempAreaProductClickCountTable(SQLContext sqlContext) {
		String sql = 
				"SELECT "
					+ "area,"
					+ "product_id,"
					+ "count(*) click_count,"
					+ "group_concat_distinct(concat_long_String(city_id,city_name,':')) city_infos "
				+ "FROM tmp_click_product_basic "
				+ "GROUP BY area,product_id";
		
		/**
		 * double group by
		 */
		
//		String _sql = 
//				"SELECT "
//					+ "product_id_area,"
//					+ "count(click_count) click_count,"
//					+ "group_concat_distinct(city_infos) city_infos "
//				+ "FROM ( "
//					+ "SELECT "
//						+ "remove_random_prefix(product_id_area) product_id_area,"
//						+ "click_count,"
//						+ "city_infos "
//					+ "FROM ( "
//						+ "SELECT "
//							+ "product_id_area,"
//							+ "count(*) click_count,"
//							+ "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " 
//						+ "FROM ( "
//							+ "SELECT "  
//								+ "random_prefix(concat_long_string(product_id,area,':'), 10) product_id_area,"
//								+ "city_id,"
//								+ "city_name "
//							+ "FROM tmp_click_product_basic "
//						+ ") t1 "
//						+ "GROUP BY product_id_area "
//					+ ") t2 "  
//				+ ") t3 "
//				+ "GROUP BY product_id_area "; 
		
		DataFrame df = sqlContext.sql(sql);
		df.registerTempTable("tmp_area_product_click_count");
	}
	
	private static void generateTempAreaProductFullProductClickCountTable(SQLContext sqlContext){
		//use product_id get product_name and product_status
		String sql = 
				"SELECT "
					+ "tapcc.area,"
					+ "tapcc.product_id,"
					+ "tapcc.click_count,"
					+ "tapcc.city_infos,"
					+ "pi.product_name,"
					+ "if(get_json_object(pi.extend_info,'product_status')='0','self','Third Party') product_status "
				+ "FROM tmp_area_product_click_count tapcc "
				+ "JOIN product_info pi ON tapcc.product_id=pi.product_id";

		DataFrame df = sqlContext.sql(sql);
		System.out.println("tmp_area_fullprod_click_count");
		df.show();
		
		df.registerTempTable("tmp_area_fullprod_click_count");
	}

	private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {
		String sql = 
				"SELECT "
					+ "area,"
					+ "CASE "
						+ "WHEN area='China North' OR area='China East' THEN 'A level' "
						+ "WHEN area='China South' OR area='China Middle' THEN 'B level' "
						+ "WHEN area='West North' OR area='West South' THEN 'C level' "
						+ "ELSE 'D level' "
					+ "END area_level,"
					+ "product_id,"
					+ "click_count,"
					+ "city_infos,"
					+ "product_name,"
					+ "product_status "
				+ "FROM ("
					+ "SELECT "
						+ "area,"
						+ "product_id,"
						+ "click_count,"
						+ "city_infos,"
						+ "product_name,"
						+ "product_status,"
						+ "ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
					+ "FROM tmp_area_fullprod_click_count "
				+ ") t "
				+ "WHERE rank<=3";
		
		DataFrame df = sqlContext.sql(sql);
		System.out.println("window function：");
		df.show();
		return df.javaRDD();		
	}
	
	private static void persistAreaTop3Product(long taskid, List<Row> rows) {
		List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();
		
		for(Row row : rows) {
			AreaTop3Product areaTop3Product = new AreaTop3Product();
			areaTop3Product.setTaskid(taskid); 
			areaTop3Product.setArea(row.getString(0));  
			areaTop3Product.setAreaLevel(row.getString(1));  
			areaTop3Product.setProductid(row.getLong(2)); 
			areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));    
			areaTop3Product.setCityInfos(row.getString(4));  
			areaTop3Product.setProductName(row.getString(5));  
			areaTop3Product.setProductStatus(row.getString(6));  
			areaTop3Products.add(areaTop3Product);
		}
		
		IAreaTop3ProductDAO areTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
		areTop3ProductDAO.insertBatch(areaTop3Products);
	}
}
