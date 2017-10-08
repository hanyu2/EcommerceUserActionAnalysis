package me.hanyu.spark.dao.impl;

import me.hanyu.spark.dao.ITop10SessionDAO;
import me.hanyu.spark.domain.Top10Session;
import me.hanyu.spark.jdbc.JDBCHelper;

public class Top10SessionImpl implements ITop10SessionDAO {
	public void insert(Top10Session top10Session) {
		String sql = "insert into top10_session values(?,?,?,?)";
		
		Object[] params = new Object[] {top10Session.getTaskid(),
				top10Session.getCategoryid(),
				top10Session.getSessionid(),
				top10Session.getClickCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
}
