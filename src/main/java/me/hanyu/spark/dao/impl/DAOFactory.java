package me.hanyu.spark.dao.impl;

import me.hanyu.spark.dao.ISessionAggrStatDAO;
import me.hanyu.spark.dao.ISessionRandomExtractDAO;
import me.hanyu.spark.dao.ITaskDAO;

public class DAOFactory {
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}
	public static ISessionAggrStatDAO getSessionAggrStatDAO(){
		return new SessionAggrStatDAOImpl();
	}
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
		return new SessionRandomExtractDAOImpl();
	}
	public static ISessionDetailDAO getSessionDetailDAO(){
		return new SessionDetailDAOImpl();
	}
}
