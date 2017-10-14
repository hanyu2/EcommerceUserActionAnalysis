package me.hanyu.spark.dao.factory;

import me.hanyu.spark.dao.IAdBlacklistDAO;
import me.hanyu.spark.dao.IAdClickTrendDAO;
import me.hanyu.spark.dao.IAdProvinceTop3DAO;
import me.hanyu.spark.dao.IAdStatDAO;
import me.hanyu.spark.dao.IAdUserClickCountDAO;
import me.hanyu.spark.dao.IAreaTop3ProductDAO;
import me.hanyu.spark.dao.IPageSplitConvertRateDAO;
import me.hanyu.spark.dao.ISessionAggrStatDAO;
import me.hanyu.spark.dao.ISessionDetailDAO;
import me.hanyu.spark.dao.ISessionRandomExtractDAO;
import me.hanyu.spark.dao.ITaskDAO;
import me.hanyu.spark.dao.ITop10CategoryDAO;
import me.hanyu.spark.dao.ITop10SessionDAO;
import me.hanyu.spark.dao.impl.AdBlacklistDAOImpl;
import me.hanyu.spark.dao.impl.AdClickTrendDAOImpl;
import me.hanyu.spark.dao.impl.AdProvinceTop3DAOImpl;
import me.hanyu.spark.dao.impl.AdStatDAOImpl;
import me.hanyu.spark.dao.impl.AdUserClickCountDAOImpl;
import me.hanyu.spark.dao.impl.AreaTop3ProductDAOImpl;
import me.hanyu.spark.dao.impl.PageSplitConvertRateDAOImpl;
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
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}
	public static IAdStatDAO getAdStatDAO(){
		return new AdStatDAOImpl();
	}
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO(){
		return new AdProvinceTop3DAOImpl();
	}
	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}
}
