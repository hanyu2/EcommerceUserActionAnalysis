package me.hanyu.spark.dao;

import java.util.List;

import me.hanyu.spark.domain.AdStat;

public interface IAdStatDAO {
	void updateBatch(List<AdStat> adStats);
}
