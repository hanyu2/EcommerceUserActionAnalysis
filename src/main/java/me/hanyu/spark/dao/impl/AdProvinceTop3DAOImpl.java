package me.hanyu.spark.dao.impl;

import java.util.ArrayList;
import java.util.List;

import me.hanyu.spark.dao.IAdProvinceTop3DAO;
import me.hanyu.spark.domain.AdProvinceTop3;
import me.hanyu.spark.jdbc.JDBCHelper;

public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO {
	public void updateBatch(List<AdProvinceTop3> adProvinceTop3s) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		List<String> dateProvinces = new ArrayList<String>();
		
		for(AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
			String date = adProvinceTop3.getDate();
			String province = adProvinceTop3.getProvince();
			String key = date + "_" + province;
			
			if(!dateProvinces.contains(key)) {
				dateProvinces.add(key);
			}
		}
		
		String deleteSQL = "DELETE FROM ad_province_top3 WHERE date=? AND province=?";
		
		List<Object[]> deleteParamsList = new ArrayList<Object[]>();
		
		for(String dateprovince : dateProvinces) {
			String[] dateProvinceSplited = dateprovince.split("_");
			String date = dateProvinceSplited[0];
			String province = dateProvinceSplited[1];
			
			Object[] params = new Object[]{date,province};
			deleteParamsList.add(params);
		}
		
		jdbcHelper.executeBatch(deleteSQL, deleteParamsList);
		
		//insert all
		String insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)";
		
		List<Object[]> insertParamsList = new ArrayList<Object[]>();
		
		for(AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
			Object[] params = new Object[] {adProvinceTop3.getDate(),
					adProvinceTop3.getProvince(),
					adProvinceTop3.getAdid(),
					adProvinceTop3.getClickCount()};
			
			insertParamsList.add(params);
		}
		
		jdbcHelper.executeBatch(insertSQL, insertParamsList);
	}
}
