package me.hanyu.spark.dao;

import java.util.List;

import me.hanyu.spark.domain.AdBlacklist;

public interface IAdBlacklistDAO {
	void insertBatch(List<AdBlacklist> adBlacklists);
	List<AdBlacklist> findAll();
}
