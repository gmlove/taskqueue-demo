package com.brightliao.taskqueue;

import static com.brightliao.taskqueue.TaskQueue.HEARTBEAT_INTERVAL;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

@Slf4j
@Component
public class TaskQueueConsumer implements InitializingBean {

    private final TaskQueue queue;
    private final int tasksToFetchPerTime;
    private final Map<String, TaskRunnable> registeredTasks = new HashMap<>();
    private final Object consumerThreadCoordinator = new Object();
    private ConcurrentLinkedDeque<Long> runningTaskIds = new ConcurrentLinkedDeque<>();
    private boolean isStopping = false;
    private Thread consumerThread;

    public TaskQueueConsumer(TaskQueue queue, @Value("${task.tasksToFetchPerTime}") int tasksToFetchPerTime) {
        this.queue = queue;
        this.tasksToFetchPerTime = tasksToFetchPerTime;
        queue.onNewTask(this::notifyNewTask);
    }

    public void registerTask(String taskType, TaskRunnable taskRunnable) {
        if (registeredTasks.containsKey(taskType)) {
            throw new RuntimeException("task has been registered already: " + taskType);
        }
        registeredTasks.put(taskType, taskRunnable);
    }

    public void start() {
        consumerThread = new Thread(() -> {
            while (!isStopping) {
                log.info("start to find new tasks");
                var tasks = queue.popTasks(tasksToFetchPerTime);
                if (tasks.isEmpty()) {
                    try {
                        log.info("no new tasks found, will wait for next round to fetch tasks.");
                        synchronized (consumerThreadCoordinator) {
                            consumerThreadCoordinator.wait();
                        }
                        continue;
                    } catch (InterruptedException e) {
                        log.warn("Thread interrupted unexpectedly, will continue to run new tasks.", e);
                        continue;
                    }
                }
                log.info("found {} tasks.", tasks.size());
                runningTaskIds.addAll(tasks.stream().map(Task::getId).collect(Collectors.toList()));
                for (Task task : tasks) {
                    try {
                        log.info("start to run task {}(id={}).", task.getType(), task.getId());
                        queue.markStarted(task);
                        registeredTasks.get(task.getType()).run(task.getArg());
                        queue.markSucceeded(task);
                        log.info("run task {}(id={}) succeeded.", task.getType(), task.getId());
                    } catch (Exception e) {
                        queue.markFailed(task, e);
                        log.warn("run task {}(id={}) failed.", task.getType(), task.getId(), e);
                    } finally {
                        runningTaskIds.remove(task.getId());
                    }
                }
            }
        });
        consumerThread.setDaemon(false);
        consumerThread.start();
    }

    @Scheduled(fixedRate = 10 * 1000)
    public void notifyNewTask() {
        synchronized (consumerThreadCoordinator) {
            log.info("notify consumer of new tasks");
            consumerThreadCoordinator.notifyAll();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    @Scheduled(fixedRate = HEARTBEAT_INTERVAL)
    public void triggerHeartBeat() {
        queue.heartbeat(runningTaskIds);
    }
}
