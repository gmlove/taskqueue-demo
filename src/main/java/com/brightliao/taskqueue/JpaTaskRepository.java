package com.brightliao.taskqueue;

import com.brightliao.taskqueue.Task.TaskStatus;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
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
        return entityMapper.toTask(taskRepository.save(entityMapper.fromTask(task)));
    }

    @Override
    public List<Task> saveAll(List<Task> tasks) {
        final List<TaskEntity> taskEntities = tasks.stream().map(entityMapper::fromTask).collect(Collectors.toList());
        return taskRepository.saveAll(taskEntities).stream()
                .map(entityMapper::toTask)
                .collect(Collectors.toList());
    }

    @Repository
    public interface InnerJpaTaskRepository extends JpaRepository<TaskEntity, Long> {
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
        private LocalDateTime createdAt;
        private LocalDateTime startedAt;
        private LocalDateTime runAt;
        private LocalDateTime endedAt;
    }
}
