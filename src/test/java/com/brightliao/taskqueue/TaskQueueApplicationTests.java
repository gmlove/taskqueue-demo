package com.brightliao.taskqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
    void should_runTask() throws InterruptedException {
        queueConsumer.registerTask("task_1", argString -> {
            try {
                var arg = objectMapper.readValue(argString, TaskArg.class);
                System.out.println("run task_1 with arg: " + arg);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        queue.addTask("task_1", new TaskArg("some message"));

        Thread.sleep(10000);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TaskArg {

        private String message;
    }
}
