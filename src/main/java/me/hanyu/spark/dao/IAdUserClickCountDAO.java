package me.hanyu.spark.dao;

import java.util.List;

import me.hanyu.spark.domain.AdUserClickCount;

public interface IAdUserClickCountDAO {
	void updateBatch(List<AdUserClickCount> adUserClickCounts);
	int findClickCountByMultiKey(String date, long userid, long adid);
}
