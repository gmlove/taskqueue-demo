A TDD demo
-----

This demo is created for demonstrating how to use TDD to implement a long-running task module in a spring-based web system based on database.

In this demo, we'll see:

- How does TDD help to finish our design.
- How does TDD help to create just-right components and APIs.
- How does TDD help to encourage us to use more domain language in our code.
- How does TDD provide security fence to give us confidence of our code.
- How does TDD help us fix potential bugs in code.

## Basic analysis and design

- Start a background thread to handle tasks
- Use scheduling infrastructure provided by spring to trigger task periodically
- Use database transaction and version to handle multi-processes case
- Should clean zombie tasks and restart it automatically

## Tests covered

### Case 1: should run tasks

- Register task handler in consumer
- Add task to queue
- Task should be saved to database
- Consumer should fetch new tasks and run it
- The task should be handled after a while

### Case 2: consumer should be waked up periodically

- Start one consumer
- Wait for a period to see if any logs printed to show that the consumer is fetching new tasks from the queue

### Case 3: consumer should be waked up immediately if there is a new task added

- Start one consumer
- Wait until the consumer found non tasks and change to sleeping status
- Add a task to the queue
- Check if the consumer is wake up immediately to handle the new task 

### Case 4: zombie tasks should be cleaned and restart in other consumer

- Start one consumer
- Add 1 long-running task
- After the task added to the queue, kill the consumer process
- Start another consumer
- Wait until the other consumer to clean zombie tasks and pick up the cleaned task

