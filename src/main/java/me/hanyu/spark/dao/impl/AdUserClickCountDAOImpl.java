package me.hanyu.spark.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import me.hanyu.spark.dao.IAdUserClickCountDAO;
import me.hanyu.spark.domain.AdUserClickCount;
import me.hanyu.spark.jdbc.JDBCHelper;
import me.hanyu.spark.model.AdUserClickCountQueryResult;

public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO{

	public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		//split click to toBeInserted and toBeUpdated
		List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<AdUserClickCount>();
		List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<AdUserClickCount>();
		
		String selectSQL = "SELECT count(*) FROM ad_user_click_count "
				+ "WHERE date=? AND user_id=? AND ad_id=? ";
		Object[] selectParams = null;
		
		for(AdUserClickCount adUserClickCount : adUserClickCounts) {
			final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();
			
			selectParams = new Object[]{adUserClickCount.getDate(), 
					adUserClickCount.getUserid(), adUserClickCount.getAdid()};
			
			jdbcHelper.executeQuery(selectSQL, selectParams, new JDBCHelper.QueryCallback() {
				
				public void process(ResultSet rs) throws Exception {
					if(rs.next()) {
						int count = rs.getInt(1);
						queryResult.setCount(count);  
					}
				}
			});
			
			int count = queryResult.getCount();
			
			if(count > 0) {
				updateAdUserClickCounts.add(adUserClickCount);
			} else {
				insertAdUserClickCounts.add(adUserClickCount);
			}
		}
		String insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)";  
		List<Object[]> insertParamsList = new ArrayList<Object[]>();
		//batch insert
		for(AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
			Object[] insertParams = new Object[]{adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid(),
					adUserClickCount.getClickCount()};  
			insertParamsList.add(insertParams);
		}
		
		jdbcHelper.executeBatch(insertSQL, insertParamsList);
		
		//batch update
		String updateSQL = "UPDATE ad_user_click_count SET click_count=click_count+? "
				+ "WHERE date=? AND user_id=? AND ad_id=? ";  
		List<Object[]> updateParamsList = new ArrayList<Object[]>();
		
		for(AdUserClickCount adUserClickCount : updateAdUserClickCounts) {
			Object[] updateParams = new Object[]{adUserClickCount.getClickCount(),
					adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid()};  
			updateParamsList.add(updateParams);
		}
		
		jdbcHelper.executeBatch(updateSQL, updateParamsList);
	}

	public int findClickCountByMultiKey(String date, long userid, long adid) {
		// TODO Auto-generated method stub
		return 0;
	}

}
