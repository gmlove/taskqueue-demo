package com.brightliao.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaskQueue {

    public static final int HEARTBEAT_INTERVAL = 10 * 1000;  // in milliseconds
    private final TaskRepository taskRepository;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;
    private ConcurrentLinkedDeque<Runnable> newTaskListeners = new ConcurrentLinkedDeque<>();

    public <T> void addTask(String taskType, T taskArg) {
        transactionTemplate.executeWithoutResult(status -> {
            try {
                taskRepository.save(new Task(taskType, objectMapper.writer().writeValueAsString(taskArg)));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        newTaskListeners.forEach(listener -> {
            try {
                listener.run();
            } catch (Exception e) {
                log.error("run new task listener failed.", e);
            }
        });
    }

    public List<Task> popTasks(int tasksToFetchPerTime) {
        return transactionTemplate.execute(status -> {
            var tasks = taskRepository.findNewTasks(tasksToFetchPerTime);
            if (tasks.isEmpty()) {
                return tasks;
            }
            tasks.forEach(Task::markStarted);
            tasks = taskRepository.saveAll(tasks);
            return tasks;
        });
    }

    public void markSucceeded(Task task) {
        transactionTemplate.executeWithoutResult(status -> {
            task.markSucceeded();
            taskRepository.save(task);
        });
    }

    public void markFailed(Task task, Exception e) {
        transactionTemplate.executeWithoutResult(status -> {
            task.markFailed(e);
            taskRepository.save(task);
        });
    }

    public void onNewTask(Runnable listener) {
        this.newTaskListeners.add(listener);
    }

    public void markStarted(Task task) {
        transactionTemplate.executeWithoutResult(status -> {
            task.markStarted();
            taskRepository.save(task);
        });
    }

    @Scheduled(fixedRate = HEARTBEAT_INTERVAL * 3, initialDelay = HEARTBEAT_INTERVAL * 3)
    public void cleanZombieTasks() {
        transactionTemplate.executeWithoutResult(status -> {
            log.info("start to clean zombie tasks.");
            int cleanedCount = taskRepository.cleanZombieTasks(HEARTBEAT_INTERVAL * 3);
            log.info("clean {} zombie tasks.", cleanedCount);
        });
    }

    public void heartbeat(ConcurrentLinkedDeque<Task> runningTasks) {
        runningTasks.forEach(Task::heartbeat);
        transactionTemplate.executeWithoutResult(status -> {
            taskRepository.saveAll(new ArrayList<>(runningTasks));
        });
    }
}
