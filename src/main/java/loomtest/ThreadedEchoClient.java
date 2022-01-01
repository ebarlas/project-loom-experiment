package loomtest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class ThreadedEchoClient {

    record Args(String host, int port, int numConnections, int contentLength, int duration, boolean loom) {
        static Args parse(String[] args) {
            return new Args(
                    args.length >= 1 ? args[0] : "localhost",
                    args.length >= 2 ? Integer.parseInt(args[1]) : 9000,
                    args.length >= 3 ? Integer.parseInt(args[2]) : 10,
                    args.length >= 4 ? Integer.parseInt(args[3]) : 32,
                    args.length >= 5 ? Integer.parseInt(args[4]) : 5_000,
                    args.length >= 6 ? Boolean.parseBoolean(args[5]) : false);
        }
    }

    static class Barrier {
        final CyclicBarrier barrier;
        final AtomicLong startTime;

        Barrier(int numConnections) {
            barrier = new CyclicBarrier(numConnections);
            startTime = new AtomicLong();
        }

        void await() throws InterruptedException, BrokenBarrierException, TimeoutException {
            barrier.await(60, TimeUnit.SECONDS);
            if (startTime.compareAndSet(0, System.currentTimeMillis())) {
                System.out.println("barrier opened!");
            }
        }

        long startTime() {
            return startTime.get();
        }
    }

    public static void main(String[] argv) throws InterruptedException, ExecutionException {
        Args args = Args.parse(argv);
        System.out.println(args);
        byte[] content = new byte[args.contentLength];
        Arrays.fill(content, (byte) 'z');
        AtomicInteger echoCount = new AtomicInteger();
        Barrier barrier = new Barrier(args.numConnections);
        ExecutorService executor = args.loom
                ? Executors.newVirtualThreadPerTaskExecutor()
                : Executors.newFixedThreadPool(args.numConnections);
        Callable<Void> task = () -> {
            Socket socket = new Socket(args.host, args.port);
            barrier.await();
            long deadline = barrier.startTime() + args.duration;
            byte[] buffer = new byte[content.length];
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            while (System.currentTimeMillis() < deadline) {
                outputStream.write(content);
                int bytesRead = 0;
                while (bytesRead < content.length) {
                    int n = inputStream.read(buffer, bytesRead, buffer.length - bytesRead);
                    if (n < 0) {
                        throw new IOException("reached end-of-stream unexpectedly");
                    }
                    bytesRead += n;
                }
                if (!Arrays.equals(content, buffer)) {
                    throw new AssertionError();
                }
                echoCount.incrementAndGet();
            }
            return null;
        };
        List<Future<Void>> futures = IntStream.range(0, args.numConnections)
                .mapToObj(n -> executor.submit(task))
                .toList();
        for (Future<Void> future : futures) {
            future.get();
        }
        long duration = System.currentTimeMillis() - barrier.startTime();
        System.out.printf("duration: %d ms, throughput: %f msg/sec\n", duration, echoCount.get() / (duration / 1000.0));
        executor.shutdown();
    }
}
