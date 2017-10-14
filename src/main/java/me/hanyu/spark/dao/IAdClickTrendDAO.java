package me.hanyu.spark.dao;

import java.util.List;

import me.hanyu.spark.domain.AdClickTrend;

public interface IAdClickTrendDAO {
	void updateBatch(List<AdClickTrend> adClickTrends);
}
