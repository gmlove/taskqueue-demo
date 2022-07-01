package com.brightliao.taskqueue;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class Task {

    private Long id;
    private String taskType;
    private String taskArg;
    private TaskStatus status;
    private String message;
    private LocalDateTime createdAt;
    private LocalDateTime startedAt;
    private LocalDateTime runAt;
    private LocalDateTime endedAt;
    private LocalDateTime heartbeatAt;

    public Task(String taskType, String taskArg) {
        this(null, taskType, taskArg, TaskStatus.PENDING);
    }

    public Task(Long id, String taskType, String taskArg, TaskStatus status) {
        this.id = id;
        this.taskType = taskType;
        this.taskArg = taskArg;
        this.status = status;
        this.createdAt = LocalDateTime.now();
    }

    public boolean isSucceeded() {
        return status == TaskStatus.SUCCEEDED;
    }

    public String getType() {
        return taskType;
    }

    public String getArg() {
        return taskArg;
    }

    public void markRunning() {
        this.status = TaskStatus.RUNNING;
        this.runAt = LocalDateTime.now();
    }

    public void markStarted() {
        this.status = TaskStatus.STARTED;
        this.startedAt = LocalDateTime.now();
    }

    public void markSucceeded() {
        this.status = TaskStatus.SUCCEEDED;
        this.endedAt = LocalDateTime.now();
    }

    public void markFailed(Exception e) {
        this.status = TaskStatus.FAILED;
        this.endedAt = LocalDateTime.now();
        this.message = this.message == null ? e.getMessage() : this.message + "\n" + e.getMessage();
    }

    public Long getId() {
        return id;
    }

    public enum TaskStatus {
        PENDING,
        STARTED,
        RUNNING,
        SUCCEEDED,
        FAILED
    }
}
