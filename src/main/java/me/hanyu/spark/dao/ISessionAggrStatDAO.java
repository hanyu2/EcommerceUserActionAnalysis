package me.hanyu.spark.dao;

import me.hanyu.spark.domain.SessionAggrStat;

//insert sesssion aggr result
public interface ISessionAggrStatDAO {
	void insert(SessionAggrStat sessionAgggrStat);
}
