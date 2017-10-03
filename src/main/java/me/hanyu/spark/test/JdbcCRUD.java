package me.hanyu.spark.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcCRUD {
	public static void main(String[] args) {
		//insert();
		//select();
		preparedStatement();
	}
	public static void insert(){
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark",
					"root",
					"root");
			stmt = conn.createStatement();
			String sql = "insert into test_user(name, age) values ('John', 25)";
			int rtn = stmt.executeUpdate(sql);
			System.out.println("Affected : " + rtn);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			try {
				if(stmt != null) {
					stmt.close();
				} 
				if(conn != null) {
					conn.close();  
				}
			} catch (Exception e2) {
				// TODO: handle exception
				e2.printStackTrace();
			}
		}
	}
	
	private static void update() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/spark_project", 
					"root", 
					"root"); 
			stmt = conn.createStatement();
			
			String sql = "update test_user set age=27 where name='John'";
			int rtn = stmt.executeUpdate(sql);
			
			System.out.println("Affected" + rtn + "lines");  
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(stmt != null) {
					stmt.close();
				} 
				if(conn != null) {
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace(); 
			}
		}
	}
	
	private static void delete() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/spark", 
					"root", 
					"root"); 
			stmt = conn.createStatement();
			
			String sql = "delete from test_user where name='John'";
			int rtn = stmt.executeUpdate(sql);
			
			System.out.println("Affected" + rtn + "lines");  
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(stmt != null) {
					stmt.close();
				} 
				if(conn != null) {
					conn.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace(); 
			}
		}
	}
	
	private static void select() {
		Connection conn = null;
		Statement stmt = null;

		ResultSet rs = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/spark", 
					"root", 
					"root"); 
			stmt = conn.createStatement();
			
			String sql = "select * from test_user";
			rs = stmt.executeQuery(sql);
			
			while(rs.next()) {
				String name = rs.getString(1);
				int age = rs.getInt(2);
				System.out.println("name=" + name + ", age=" + age);    
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(stmt != null) {
					stmt.close();
				} 
				if(conn != null) {
					conn.close();  
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	private static void preparedStatement() {
		Connection conn = null;

		PreparedStatement pstmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/spark", 
					"root", 
					"root");  
			
			String sql = "insert into test_user(name,age) values(?,?)";
			
			pstmt = conn.prepareStatement(sql);
			
			pstmt.setString(1, "Alex");  
			pstmt.setInt(2, 26);  
			
			int rtn = pstmt.executeUpdate();    
			
			System.out.println("Affected" + rtn + "lines");  
		} catch (Exception e) {
			e.printStackTrace();  
		} finally {
			try {
				if(pstmt != null) {
					pstmt.close();
				} 
				if(conn != null) {
					conn.close();  
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
} 
