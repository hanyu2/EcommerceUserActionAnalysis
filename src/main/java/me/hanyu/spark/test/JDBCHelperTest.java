package me.hanyu.spark.test;

import java.util.ArrayList;
import java.util.List;

import me.hanyu.spark.jdbc.JDBCHelper;

public class JDBCHelperTest {
	public static void main(String[] args) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();

		//jdbcHelper.executeUpdate("insert into test_user(name,age) values(?,?)", new Object[] { "Steve", 28 });
		/*final Map<String, Object> testUser = new HashMap<String, Object>();
		jdbcHelper.executeQuery("select name,age from test_user where name=?", new Object[] { "Steve" },
				new JDBCHelper.QueryCallback() {
					public void process(ResultSet rs) throws Exception {
						if (rs.next()) {
							String name = rs.getString(1);
							int age = rs.getInt(2);
							testUser.put("name", name);
							testUser.put("age", age);
						}
					}

				});
		System.out.println(testUser.get("name") + ":" + testUser.get("age"));*/
		String sql = "insert into test_user(name,age) values(?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		paramsList.add(new Object[] { "Joe", 30 });
		paramsList.add(new Object[] { "Bob", 35 });
		jdbcHelper.executeBatch(sql, paramsList);
	}
}
