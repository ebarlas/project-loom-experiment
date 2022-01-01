package loomtest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class NioEchoClient {

    record Args(String host, int port, int numConnections, int contentLength, int duration) {
        static Args parse(String[] args) {
            return new Args(
                    args.length >= 1 ? args[0] : "localhost",
                    args.length >= 2 ? Integer.parseInt(args[1]) : 9000,
                    args.length >= 3 ? Integer.parseInt(args[2]) : 10,
                    args.length >= 4 ? Integer.parseInt(args[3]) : 32,
                    args.length >= 5 ? Integer.parseInt(args[4]) : 5_000);
        }
    }

    public static void main(String[] argv) throws IOException {
        Args args = Args.parse(argv);
        System.out.println(args);
        byte[] content = new byte[args.contentLength];
        Arrays.fill(content, (byte) 'z');
        long startTime = 0;
        long deadline = Long.MAX_VALUE;
        int echoCount = 0;
        int connectionCount = 0;
        int exitCount = 0;
        Selector selector = Selector.open();
        for (int i = 0; i < args.numConnections; i++) {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.register(selector, SelectionKey.OP_CONNECT, ByteBuffer.wrap(Arrays.copyOf(content, content.length)));
            socketChannel.connect(new InetSocketAddress(args.host, args.port));
        }
        outer: while (true) {
            selector.select();
            Set<SelectionKey> selKeys = selector.selectedKeys();
            Iterator<SelectionKey> it = selKeys.iterator();
            boolean afterDeadline = System.currentTimeMillis() > deadline;
            while (it.hasNext()) {
                SelectionKey selKey = it.next();
                SocketChannel socketChannel = (SocketChannel) selKey.channel();
                if (selKey.isConnectable()) {
                    socketChannel.finishConnect();
                    connectionCount++;
                    if (connectionCount == args.numConnections) { // barrier reached, allow all connections to proceed
                        System.out.println("barrier opened!");
                        startTime = System.currentTimeMillis();
                        deadline = startTime + args.duration;
                        for (SelectionKey key : selector.keys()) {
                            SelectableChannel channel = key.channel();
                            if (channel instanceof SocketChannel sc) {
                                sc.register(selector, SelectionKey.OP_WRITE, key.attachment());
                            }
                        }
                    }
                } else if (selKey.isWritable()) {
                    ByteBuffer buffer = (ByteBuffer) selKey.attachment();
                    socketChannel.write(buffer);
                    if (!buffer.hasRemaining()) {
                        buffer.clear();
                        socketChannel.register(selector, SelectionKey.OP_READ, buffer);
                    }
                } else if (selKey.isReadable()) {
                    ByteBuffer buffer = (ByteBuffer) selKey.attachment();
                    int bytesRead = socketChannel.read(buffer);
                    if (bytesRead < 0) {
                        throw new IOException("reached end-of-stream unexpectedly");
                    }
                    if (!buffer.hasRemaining()) {
                        if (!Arrays.equals(content, buffer.array())) {
                            throw new AssertionError();
                        }
                        echoCount++;
                        if (afterDeadline) {
                            exitCount++;
                            if (exitCount == args.numConnections) {
                                break outer;
                            }
                        } else {
                            buffer.flip();
                            socketChannel.register(selector, SelectionKey.OP_WRITE, buffer);
                        }
                    }
                }
                it.remove();
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.printf("duration: %d ms, throughput: %f msg/sec\n", duration, echoCount / (duration / 1000.0));
    }
}