package me.hanyu.spark.test;

import me.hanyu.spark.dao.ITaskDAO;
import me.hanyu.spark.dao.impl.DAOFactory;
import me.hanyu.spark.domain.Task;

public class TaskDAOTest {
	public static void main(String[] args) {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());
	}
}
