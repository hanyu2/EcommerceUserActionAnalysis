package me.hanyu.spark.dao.factory;

import me.hanyu.spark.dao.ISessionAggrStatDAO;
import me.hanyu.spark.dao.ISessionDetailDAO;
import me.hanyu.spark.dao.ISessionRandomExtractDAO;
import me.hanyu.spark.dao.ITaskDAO;
import me.hanyu.spark.dao.ITop10CategoryDAO;
import me.hanyu.spark.dao.ITop10SessionDAO;
import me.hanyu.spark.dao.impl.SessionAggrStatDAOImpl;
import me.hanyu.spark.dao.impl.SessionDetailDAOImpl;
import me.hanyu.spark.dao.impl.SessionRandomExtractDAOImpl;
import me.hanyu.spark.dao.impl.TaskDAOImpl;
import me.hanyu.spark.dao.impl.Top10CategoryDAOImpl;
import me.hanyu.spark.dao.impl.Top10SessionImpl;

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
	public static ITop10CategoryDAO getTop10CategoryDAO(){
		return new Top10CategoryDAOImpl();
	}
	public static ITop10SessionDAO getTop10SessionDAO(){
		return new Top10SessionImpl();
	}
}
