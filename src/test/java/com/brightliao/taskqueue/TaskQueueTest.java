package com.brightliao.taskqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TaskQueueTest {

    @Test
    void should_run_task_from_queue() throws InterruptedException {
        var taskRepository = mock(TaskRepository.class);
        var queue = new TaskQueue(taskRepository, 1);
        var consumer = new TaskQueueConsumer(queue);

        var task1Runnable = mock(TaskRunnable.class);
        var task2Runnable = mock(TaskRunnable.class);
        consumer.registerTask("task_type_1", task1Runnable);
        consumer.registerTask("task_type_2", task2Runnable);
        consumer.start();

        // run task1 successfully
        var task1Arg = new TaskType1Arg("some arg");
        queue.addTask("task_type_1", task1Arg);

        final Task task1 = someTask(1L, "task_type_1", task1Arg);
        when(taskRepository.findNewTasks(eq(1))).thenReturn(List.of(task1));

        Thread.sleep(100);

        verify(taskRepository, times(1)).save(eq(task1));
        verify(task1Runnable, times(1)).run(eq(task1Arg));
        verify(taskRepository, times(1)).markTaskStarted(eq(List.of(1L)));
        verify(taskRepository, times(1)).markTaskSucceeded(eq(List.of(1L)));
        assertThat(task1.isSucceeded()).isEqualTo(true);

        // run task2 failed
        var task2Arg = new TaskType2Arg("some arg");
        queue.addTask("task_type_2", task2Arg);

        final Task task2 = someTask(2L, "task_type_2", task2Arg);
        when(taskRepository.findNewTasks(eq(1))).thenReturn(List.of(task2));
        doThrow(RuntimeException.class).when(task2Runnable).run(eq(task2Arg));

        Thread.sleep(100);

        verify(taskRepository, times(1)).save(eq(task1));
        verify(task2Runnable, times(1)).run(eq(task2Arg));
        verify(taskRepository, times(1)).markTaskStarted(eq(List.of(2L)));
        verify(taskRepository, times(1)).markTaskFailed(eq(List.of(2L)));
        assertThat(task2.isSucceeded()).isEqualTo(false);
    }

    private Task someTask(long id, String taskType, Object taskArg) {
        return new Task();
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
