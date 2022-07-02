package com.brightliao.taskqueue;

import static java.time.temporal.ChronoUnit.MILLIS;

import com.brightliao.taskqueue.Task.TaskStatus;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.LockModeType;
import javax.persistence.PersistenceContext;

@RequiredArgsConstructor
@Service
public class JpaTaskRepository implements TaskRepository {

    @Autowired
    private final TaskEntityMapper entityMapper;
    @Autowired
    private final InnerJpaTaskRepository taskRepository;

    private EntityManager entityManager;

    @PersistenceContext
    public final void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Override
    public List<Task> findNewTasks(int maxCount) {
        var query = entityManager.createQuery(
                "from JpaTaskRepository$TaskEntity "
                        + "where status = :status "
                        + "order by createdAt asc",
                TaskEntity.class);
        query.setParameter("status", TaskStatus.PENDING);
        query.setLockMode(LockModeType.PESSIMISTIC_WRITE);
        query.setMaxResults(maxCount);
        return query.getResultList().stream()
                .map(entityMapper::toTask)
                .collect(Collectors.toList());
    }

    @Override
    public Task save(Task task) {
        if (task.getId() != null) {
            var taskInDb = taskRepository.findById(task.getId()).orElseThrow(EntityNotFoundException::new);
            ensureVersionIsSmallerInDb(task, taskInDb);
        }
        return entityMapper.toTask(taskRepository.save(entityMapper.fromTask(task)));
    }

    private void ensureVersionIsSmallerInDb(Task task, TaskEntity taskInDb) {
        if (taskInDb.getVersion() >= task.getVersion()) {
            throw new RuntimeException(String.format("found a bigger version %s >= %s in db, will not do saving.",
                    taskInDb.getVersion(), task.getVersion()));
        }
    }

    @Override
    public List<Task> saveAll(List<Task> tasks) {
        var tasksHaveId = tasks.stream().filter(Task::hasId).collect(Collectors.toMap(Task::getId, Function.identity()));
        if (tasksHaveId.size() > 0) {
            taskRepository.findAllById(tasksHaveId.keySet())
                    .forEach(taskInDb -> ensureVersionIsSmallerInDb(tasksHaveId.get(taskInDb.getId()), taskInDb));
        }
        final List<TaskEntity> taskEntities = tasks.stream().map(entityMapper::fromTask).collect(Collectors.toList());
        return taskRepository.saveAll(taskEntities).stream()
                .map(entityMapper::toTask)
                .collect(Collectors.toList());
    }

    @Override
    public void updateHeartbeat(List<Long> taskIds) {
        if (taskIds.isEmpty()) {
            return;
        }
        taskRepository.updateHeartbeat(taskIds, LocalDateTime.now());
    }

    @Override
    public void cleanZombieTasks(long heartbeatTimeout) {
        taskRepository.cleanZombieTasks(heartbeatTimeout);
    }

    @Repository
    public interface InnerJpaTaskRepository extends JpaRepository<TaskEntity, Long> {

        @Modifying
        @Query("UPDATE JpaTaskRepository$TaskEntity t SET "
                + "status = :pendingStatus, "
                + "message = CONCAT('message', '\n', :message) "
                + "WHERE status in :runningStatus and heartbeatAt < :minHeartbeatTime")
        int cleanZombieTasks(
                @Param("minHeartbeatTime") LocalDateTime minHeartbeatTime, @Param("message") String message,
                @Param("runningStatus") List<TaskStatus> runningStatus, @Param("pendingStatus") TaskStatus pendingStatus);

        default int cleanZombieTasks(@Param("heartbeatTimeout") long heartbeatTimeout) {
            var minHeartbeatTime = LocalDateTime.now().minus(heartbeatTimeout, MILLIS);
            var message = String.format("Clean zombie task at [%s].",
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()));
            return cleanZombieTasks(minHeartbeatTime, message, List.of(TaskStatus.RUNNING, TaskStatus.STARTED), TaskStatus.PENDING);
        }

        @Modifying
        @Query("UPDATE JpaTaskRepository$TaskEntity t SET "
                + "version = version + 1, "
                + "heartbeatAt = :heartbeatTime "
                + "WHERE id in :ids")
        void updateHeartbeat(@Param("ids") List<Long> ids, @Param("heartbeatTime") LocalDateTime heartbeatTime);

    }

    @Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
    public interface TaskEntityMapper {

        TaskEntity fromTask(Task task);

        Task toTask(TaskEntity task);

    }

    @Entity
    @Data
    public static class TaskEntity {

        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        private String taskType;
        @Column(columnDefinition = "TEXT")
        private String taskArg;
        @Enumerated(EnumType.STRING)
        private TaskStatus status;
        @Column(columnDefinition = "TEXT")
        private String message;
        private int version;
        private LocalDateTime createdAt;
        private LocalDateTime startedAt;
        private LocalDateTime runAt;
        private LocalDateTime endedAt;
        private LocalDateTime heartbeatAt;
    }
}
