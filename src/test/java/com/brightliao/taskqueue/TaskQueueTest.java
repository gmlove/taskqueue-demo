package com.brightliao.taskqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.brightliao.taskqueue.Task.TaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.function.Consumer;

public class TaskQueueTest {

    @Test
    void should_run_task_from_queue() throws InterruptedException {
        var tt = mock(TransactionTemplate.class);
        when(tt.execute(any())).thenAnswer(answer ->
                ((TransactionCallback<?>) answer.getArgument(0)).doInTransaction(new SimpleTransactionStatus()));
        doAnswer(answer -> {
            ((Consumer<TransactionStatus>) answer.getArgument(0)).accept(new SimpleTransactionStatus());
            return null;
        }).when(tt).executeWithoutResult(any());

        var taskRepository = mock(TaskRepository.class);
        final ObjectMapper objectMapper = new ObjectMapper();
        var queue = new TaskQueue(taskRepository, tt, objectMapper);
        var consumer = new TaskQueueConsumer(queue, 1);

        var task1Runnable = mock(TaskRunnable.class);
        var task2Runnable = mock(TaskRunnable.class);
        consumer.registerTask("task_type_1", task1Runnable);
        consumer.registerTask("task_type_2", task2Runnable);
        consumer.start();

        // run task1 successfully
        var task1Arg = new TaskType1Arg("some arg");
        final Task task1 = someTask(1L, "task_type_1", "{\"arg\":\"some arg\"}");
        when(taskRepository.findNewTasks(eq(1))).thenReturn(List.of(task1)).thenReturn(List.of());
        when(taskRepository.saveAll(anyList())).thenAnswer(answer -> answer.getArgument(0));
        when(taskRepository.save(any())).thenAnswer(answer -> answer.getArgument(0));

        queue.addTask("task_type_1", task1Arg);

        Thread.sleep(2000);

        // add -> running -> succeeded
        verify(taskRepository, times(3)).save(any(Task.class));
        // started
        verify(taskRepository, times(1)).saveAll(anyList());
        verify(task1Runnable, times(1)).run(eq("{\"arg\":\"some arg\"}"));
        assertThat(task1.isSucceeded()).isEqualTo(true);

        // run task2 failed
        var task2Arg = new TaskType2Arg("some arg");
        final Task task2 = someTask(2L, "task_type_2", "{\"arg\":\"some arg\"}");
        when(taskRepository.findNewTasks(eq(1))).thenReturn(List.of(task2)).thenReturn(List.of());
        ;
        doThrow(RuntimeException.class).when(task2Runnable).run(eq("{\"arg\":\"some arg\"}"));

        queue.addTask("task_type_2", task2Arg);

        Thread.sleep(2000);

        // add -> running -> failed
        verify(taskRepository, times(6)).save(any(Task.class));
        // started
        verify(taskRepository, times(2)).saveAll(anyList());
        verify(task2Runnable, times(1)).run(eq("{\"arg\":\"some arg\"}"));
        assertThat(task2.isSucceeded()).isEqualTo(false);

        consumer.triggerHeartBeat();
        verify(taskRepository, times(1)).updateHeartbeat(anyList());

        queue.cleanZombieTasks();
        verify(taskRepository, times(1)).cleanZombieTasks(anyLong());
    }

    private Task someTask(long id, String taskType, String taskArg) {
        return new Task(id, taskType, taskArg, TaskStatus.PENDING);
    }

    @Data
    @AllArgsConstructor
    private class TaskType1Arg {

        private String arg;
    }

    @Data
    @AllArgsConstructor
    private class TaskType2Arg {

        private String arg;
    }
}
