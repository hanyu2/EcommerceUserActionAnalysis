package me.hanyu.spark.dao;

import java.util.List;

import me.hanyu.spark.domain.AdProvinceTop3;

public interface IAdProvinceTop3DAO {
	void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
}
