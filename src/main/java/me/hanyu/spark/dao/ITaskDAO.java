package me.hanyu.spark.dao;

import me.hanyu.spark.domain.Task;

public interface ITaskDAO {
	Task findById(long taksid);
}
