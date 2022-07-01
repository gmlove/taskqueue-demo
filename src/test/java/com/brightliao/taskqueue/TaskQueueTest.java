package com.brightliao.taskqueue;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.Test;

public class TaskQueueTest {

    @Test
    void should_run_task_from_queue() throws InterruptedException {
        var queue = new TaskQueue();
        var consumer = new TaskQueueConsumer(queue);

        var task1Runnable = mock(TaskRunnable.class);
        var task2Runnable = mock(TaskRunnable.class);
        consumer.registerTask("task_type_1", task1Runnable);
        consumer.registerTask("task_type_2", task2Runnable);
        consumer.start();

        var task1Arg = new TaskType1Arg("some arg");
        queue.addTask("task_type_1", task1Arg);

        Thread.sleep(100);

        verify(task1Runnable, times(1)).run(eq(task1Arg));

        var task2Arg = new TaskType2Arg("some arg");
        queue.addTask("task_type_2", task2Arg);

        Thread.sleep(100);

        verify(task2Runnable, times(1)).run(eq(task2Arg));
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
