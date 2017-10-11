package me.hanyu.spark.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import me.hanyu.spark.conf.ConfigurationManager;
import me.hanyu.spark.constant.Constants;

//jdbc connection pool
public class JDBCHelper {
	static {
		try {
			String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
			Class.forName(driver);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	private static JDBCHelper instance = null;

	public static JDBCHelper getInstance() {
		if (instance == null) {
			synchronized (JDBCHelper.class) {
				if (instance == null) {
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}

	private LinkedList<Connection> datasource = new LinkedList<Connection>();

	private JDBCHelper() {
		int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
		for (int i = 0; i < datasourceSize; i++) {
			boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
			String url = null;
			String user = null;
			String password = null;
			if (local) {
				url = ConfigurationManager.getProperty(Constants.JDBC_URL);
				user = ConfigurationManager.getProperty(Constants.JDBC_USER);
				password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
			} else {
				url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
				user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
				password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
			}
			try {
				Connection conn = DriverManager.getConnection(url, user, password);
				datasource.push(conn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public synchronized Connection getConnection() {
		while (datasource.size() == 0) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return datasource.poll();
	}

	// return affected lines
	public int executeUpdate(String sql, Object[] params) {
		int rtn = 0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				pstmt.setObject(i + 1, params[i]);
			}
			rtn = pstmt.executeUpdate();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			if (conn != null) {
				datasource.push(conn);
			}
		}
		return rtn;
	}

	public void executeQuery(String sql, Object[] params, QueryCallback callback) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);

			for (int i = 0; i < params.length; i++) {
				pstmt.setObject(i + 1, params[i]);
			}
			rs = pstmt.executeQuery();
			callback.process(rs);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				datasource.push(conn);
			}
		}
	}

	public int[] executeBatch(String sql, List<Object[]> paramsList) {
		int[] rtn = null;
		Connection conn = null;
		PreparedStatement pstmt = null;

		try {
			conn = getConnection();

			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);

			for (Object[] params : paramsList) {
				for (int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);
				}
				pstmt.addBatch();
			}

			rtn = pstmt.executeBatch();

			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return rtn;
	}

	public static interface QueryCallback {
		void process(ResultSet rs) throws Exception;
	}
}
