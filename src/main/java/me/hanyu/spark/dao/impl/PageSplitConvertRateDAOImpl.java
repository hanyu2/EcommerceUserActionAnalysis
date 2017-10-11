package me.hanyu.spark.dao.impl;

import me.hanyu.spark.dao.IPageSplitConvertRateDAO;
import me.hanyu.spark.domain.PageSplitConvertRate;
import me.hanyu.spark.jdbc.JDBCHelper;

public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO{
	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate values(?,?)";
		Object[] params = new Object[] {pageSplitConvertRate.getTaskid(),
				pageSplitConvertRate.getConvertRate()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
}
