package com.brightliao.taskqueue;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class TaskQueueApplicationTests {

    @Autowired
    private TaskQueue queue;
    @Autowired
    private TaskQueueConsumer queueConsumer;
    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void should_runTask_periodically_and_fetchNewTasks_immediately_after_new_task_added() throws InterruptedException {
        queueConsumer.registerTask("task_1", argString -> {
            try {
                var arg = objectMapper.readValue(argString, TaskArg.class);
                System.out.println("run task_1 with arg: " + arg);
                Thread.sleep(1000);
            } catch (JsonProcessingException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        queue.addTask("task_1", new TaskArg("some message"));

        Thread.sleep(5000);

        assertThat(queueConsumer.isWaiting()).isTrue();
        System.out.println("consumer should be in sleeping status, try add a new task and see if consumer wake up immediately");
        queue.addTask("task_1", new TaskArg("some message 1"));
        Thread.sleep(10);
        assertThat(queueConsumer.isRunning()).isTrue();

        Thread.sleep(5000);
    }

    @Test
    @Disabled("run this test manually and run part 2 after it exists")
    void should_clean_zombie_tasks_and_start_to_handle_new_tasks_part_1_generate_zombie_task() throws InterruptedException {
        registerTask();
        queue.addTask("task_1", new TaskArg("some message"));
        Thread.sleep(5000);
        System.exit(0);
    }

    private void registerTask() {
        queueConsumer.registerTask("task_1", argString -> {
            try {
                var arg = objectMapper.readValue(argString, TaskArg.class);
                System.out.println("run task_1 with arg: " + arg);
                Thread.sleep(10000);
            } catch (JsonProcessingException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    @Disabled("run this manually after part 1")
    void should_clean_zombie_tasks_and_start_to_handle_new_tasks_part_2_clean_and_run_zombie_task() throws InterruptedException {
        registerTask();
        // system should trigger zombie tasks cleaning after waiting for this long
        Thread.sleep(TaskQueue.HEARTBEAT_INTERVAL * 3 + 1000);

        System.out.println("consumer might be in sleeping status, try notify new task and see if consumer wake up immediately");
        queueConsumer.notifyNewTask();
        Thread.sleep(10);
        assertThat(queueConsumer.isRunning()).isTrue();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TaskArg {

        private String message;
    }
}
