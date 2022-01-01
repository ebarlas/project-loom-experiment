package loomtest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

public class NioEchoServer {

    record Args(String host, int port, int bufferSize, int latency, int resolution, int acceptQueueLen) {
        static Args parse(String[] args) {
            return new Args(
                    args.length >= 1 ? args[0] : "localhost",
                    args.length >= 2 ? Integer.parseInt(args[1]) : 9000,
                    args.length >= 3 ? Integer.parseInt(args[2]) : 32,
                    args.length >= 4 ? Integer.parseInt(args[3]) : 1000,
                    args.length >= 5 ? Integer.parseInt(args[4]) : 10,
                    args.length >= 6 ? Integer.parseInt(args[5]) : 1_024);
        }
    }

    /**
     * Queue of delayed tasks. Each task has a target execution time. The delay period is fixed, so target time
     * increases from front to back. Therefore, only candidate tasks at the head of the queue ought to be considered
     * for execution.
     */
    static class DelayQueue {
        final Queue<DelayedTask> delayQueue;
        final long delay;

        record DelayedTask(Callable<Void> task, long time) {
        }

        DelayQueue(long delay) {
            this.delayQueue = new LinkedList<>();
            this.delay = delay;
        }

        void add(Callable<Void> task) {
            delayQueue.offer(new DelayedTask(task, System.currentTimeMillis() + delay));
        }

        void execute() throws Exception {
            DelayedTask t;
            long now = System.currentTimeMillis();
            while ((t = delayQueue.peek()) != null && t.time < now) {
                delayQueue.remove().task.call();
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        Args args = Args.parse(argv);
        System.out.println(args);
        DelayQueue delayQueue = new DelayQueue(args.latency);
        Selector selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(args.host, args.port), args.acceptQueueLen);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        int connectionCounter = 0;
        while (true) {
            selector.select(args.resolution); // select with timeout to ensure delay queue is attended to with a reasonable frequency
            delayQueue.execute();
            Set<SelectionKey> selKeys = selector.selectedKeys();
            Iterator<SelectionKey> it = selKeys.iterator();
            while (it.hasNext()) {
                SelectionKey selKey = it.next();
                if (selKey.isAcceptable()) {
                    SocketChannel socketChannel = serverSocketChannel.accept();
                    socketChannel.configureBlocking(false);
                    socketChannel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(args.bufferSize));
                    int remotePort = ((InetSocketAddress) socketChannel.socket().getRemoteSocketAddress()).getPort();
                    System.out.printf("accepted connection, count=%d, remotePort=%d\n", ++connectionCounter, remotePort);
                } else if (selKey.isReadable()) {
                    SocketChannel socketChannel = (SocketChannel) selKey.channel();
                    ByteBuffer buffer = (ByteBuffer) selKey.attachment();
                    int bytesRead;
                    try {
                        bytesRead = socketChannel.read(buffer);
                    } catch (IOException e) {
                        bytesRead = -1;
                    }
                    if (bytesRead < 0) {
                        socketChannel.close();
                        selKey.cancel();
                        int remotePort = ((InetSocketAddress) socketChannel.socket().getRemoteSocketAddress()).getPort();
                        System.out.printf("closed connection, remotePort=%d\n", remotePort);
                    } else if (bytesRead > 0) {
                        buffer.flip();
                        socketChannel.register(selector, 0, buffer); // interested ops cycle: 1. READ -> 2. NONE -> 3. WRITE -> 1...
                        delayQueue.add(() -> {
                            socketChannel.register(selector, SelectionKey.OP_WRITE, buffer);
                            return null;
                        });
                    }
                } else if (selKey.isWritable()) {
                    SocketChannel socketChannel = (SocketChannel) selKey.channel();
                    ByteBuffer buffer = (ByteBuffer) selKey.attachment();
                    socketChannel.write(buffer);
                    if (!buffer.hasRemaining()) {
                        buffer.clear();
                        socketChannel.register(selector, SelectionKey.OP_READ, buffer);
                    }
                }
                it.remove();
            }
        }
    }
}