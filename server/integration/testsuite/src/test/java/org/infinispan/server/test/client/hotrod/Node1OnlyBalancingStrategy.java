package org.infinispan.server.test.client.hotrod;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.http.annotation.ThreadSafe;
import org.infinispan.client.hotrod.impl.transport.tcp.RequestBalancingStrategy;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/*
 * Load balancing strategy which always sends to node0
 */
@ThreadSafe
public class Node1OnlyBalancingStrategy implements RequestBalancingStrategy {

    private static final Log log = LogFactory.getLog(Node1OnlyBalancingStrategy.class);

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    private volatile InetSocketAddress[] servers;

    private static String host;
    private static int port = 11322;
    private static InetSocketAddress server;

    static {
        host = System.getProperty("jbosstest.cluster.node1", "localhost");
        server = new InetSocketAddress(host, port);
        if (log.isDebugEnabled())
            log.trace("node1 server = " + server);
    }

    /*
     * This gets called by ReemoteCache upon receiving a toplogy update, among other callers.
     */
    @Override
    public void setServers(Collection<SocketAddress> servers) {
        writeLock.lock();
        try {
            this.servers = servers.toArray(new InetSocketAddress[servers.size()]);
            if (log.isTraceEnabled()) {
                log.trace("New server list is: " + Arrays.toString(this.servers));
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Multiple threads might call this method at the same time.
     */
    @Override
    public SocketAddress nextServer(Set<SocketAddress> failedServers) {
        readLock.lock();
        try {
            return server;
        } finally {
            readLock.unlock();
        }
    }
}
