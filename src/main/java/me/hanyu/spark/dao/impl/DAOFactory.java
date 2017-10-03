package me.hanyu.spark.dao.impl;

import me.hanyu.spark.dao.ITaskDAO;

public class DAOFactory {
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}
}
