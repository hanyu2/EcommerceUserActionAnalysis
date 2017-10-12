package me.hanyu.spark.dao;

import java.util.List;

import me.hanyu.spark.domain.AreaTop3Product;

public interface IAreaTop3ProductDAO {
	void insertBatch(List<AreaTop3Product> areaTop3Products);
}
