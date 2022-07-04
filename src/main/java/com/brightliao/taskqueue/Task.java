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
    private int version;
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
        this.version = 0;
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
        this.heartbeatAt = LocalDateTime.now();
        version += 1;
    }

    public void markStarted() {
        this.status = TaskStatus.STARTED;
        this.startedAt = LocalDateTime.now();
        this.heartbeatAt = LocalDateTime.now();
        version += 1;
    }

    public void markSucceeded() {
        this.status = TaskStatus.SUCCEEDED;
        this.endedAt = LocalDateTime.now();
        this.heartbeatAt = LocalDateTime.now();
        version += 1;
    }

    public void markFailed(Exception e) {
        this.status = TaskStatus.FAILED;
        this.endedAt = LocalDateTime.now();
        this.heartbeatAt = LocalDateTime.now();
        this.message = this.message == null ? e.getMessage() : this.message + "\n" + e.getMessage();
        version += 1;
    }

    public void heartbeat() {
        this.heartbeatAt = LocalDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public boolean hasId() {
        return id != null;
    }

    public enum TaskStatus {
        PENDING,
        STARTED,
        RUNNING,
        SUCCEEDED,
        FAILED
    }
}
