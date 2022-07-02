package com.brightliao.taskqueue;

import java.util.List;

public interface TaskRepository {

    List<Task> findNewTasks(int maxCount);

    Task save(Task task);

    List<Task> saveAll(List<Task> tasks);

    int cleanZombieTasks(long heartbeatTimeout);
}
