package me.hanyu.spark.dao.impl;

import me.hanyu.spark.dao.ITop10CategoryDAO;
import me.hanyu.spark.domain.Top10Category;
import me.hanyu.spark.jdbc.JDBCHelper;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

	public void insert(Top10Category category) {
		String sql = "insert into top10_category values(?,?,?,?,?);";
		Object[] params = new Object[] {category.getTaskid(),
				category.getCategoryid(),
				category.getClickCount(),
				category.getOrderCount(),
				category.getPayCount(),
			};
			
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
