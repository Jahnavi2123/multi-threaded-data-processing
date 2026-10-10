import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class DataProcessingSystem {
    private static final Logger LOG =
            Logger.getLogger(DataProcessingSystem.class.getName());

    private static final int TASK_COUNT = 20;
    private static final int WORKER_COUNT = 3;

    record Task(int id, int value, boolean stop) {
        static Task poisonPill() {
            return new Task(-1, 0, true);
        }
    }

    record Result(int taskId, int input, int output, String worker) {}

    static class SharedTaskQueue {
        private final BlockingQueue<Task> queue =
                new LinkedBlockingQueue<>();

        public void addTask(Task task) throws InterruptedException {
            queue.put(task);
        }

        public Task getTask() throws InterruptedException {
            return queue.take();
        }
    }

    static class SharedResults {
        private final List<Result> results = new ArrayList<>();

        public synchronized void addResult(Result result) {
            results.add(result);
        }

        public synchronized List<Result> getResults() {
            return new ArrayList<>(results);
        }
    }

    static class Worker implements Runnable {
        private final int workerId;
        private final SharedTaskQueue queue;
        private final SharedResults results;

        Worker(int workerId, SharedTaskQueue queue,
               SharedResults results) {
            this.workerId = workerId;
            this.queue = queue;
            this.results = results;
        }

        @Override
        public void run() {
            String name = "Worker-" + workerId;
            LOG.info(name + " started");

            try {
                while (true) {
                    Task task = queue.getTask();

                    if (task.stop()) {
                        break;
                    }

                    Thread.sleep(100);

                    int output = task.value() * task.value();
                    results.addResult(new Result(
                            task.id(), task.value(), output, name));

                    LOG.info(name + " completed Task "
                            + task.id() + " -> " + output);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warning(name + " interrupted: " + e.getMessage());
            } catch (RuntimeException e) {
                LOG.severe(name + " processing error: " + e.getMessage());
            } finally {
                LOG.info(name + " stopped");
            }
        }
    }

    static void saveResults(List<Result> results, Path file)
            throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            writer.write("task_id,input,output,worker");
            writer.newLine();

            for (Result result : results) {
                writer.write(result.taskId() + ","
                        + result.input() + ","
                        + result.output() + ","
                        + result.worker());
                writer.newLine();
            }
        }
    }

    public static void main(String[] args) {
        SharedTaskQueue queue = new SharedTaskQueue();
        SharedResults sharedResults = new SharedResults();

        ExecutorService executor =
                Executors.newFixedThreadPool(WORKER_COUNT);

        boolean interrupted = false;

        try {
            for (int i = 1; i <= WORKER_COUNT; i++) {
                executor.execute(new Worker(i, queue, sharedResults));
            }

            for (int i = 1; i <= TASK_COUNT; i++) {
                queue.addTask(new Task(i, i, false));
            }

            for (int i = 0; i < WORKER_COUNT; i++) {
                queue.addTask(Task.poisonPill());
            }
        } catch (InterruptedException e) {
            interrupted = true;
            LOG.severe("Task submission interrupted: " + e.getMessage());
        } finally {
            executor.shutdown();

            if (interrupted) {
                executor.shutdownNow();
            }
        }

        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warning("Workers timed out; forcing shutdown");
                executor.shutdownNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "Workers did not terminate");
                }
            }

            List<Result> results = sharedResults.getResults();
            results.sort(Comparator.comparingInt(Result::taskId));

            Set<Integer> uniqueIds = new HashSet<>();
            for (Result result : results) {
                if (!uniqueIds.add(result.taskId())) {
                    throw new IllegalStateException(
                            "Duplicate task: " + result.taskId());
                }
            }

            if (results.size() != TASK_COUNT) {
                throw new IllegalStateException(
                        "Expected " + TASK_COUNT
                        + " results, received " + results.size());
            }

            saveResults(results, Path.of("results.csv"));

            System.out.println("\nJAVA PROCESSING SUMMARY");
            System.out.println("Tasks submitted: " + TASK_COUNT);
            System.out.println("Results saved: " + results.size());
            System.out.println("Unique task IDs: " + uniqueIds.size());
            System.out.println("Output file: results.csv");
            System.out.println("Status: SUCCESS");

        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            LOG.severe("Shutdown interrupted: " + e.getMessage());
            System.exit(1);
        } catch (IOException | RuntimeException e) {
            LOG.severe("Processing failed: " + e.getMessage());
            System.exit(1);
        }
    }
}
