package org.infinispan.client.hotrod.impl.transport.tcp;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Round-robin implementation for {@link org.infinispan.client.hotrod.impl.transport.tcp.RequestBalancingStrategy}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
public class RoundRobinBalancingStrategy implements RequestBalancingStrategy {

   private static Log log = LogFactory.getLog(RoundRobinBalancingStrategy.class);

   private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
   private final Lock readLock = readWriteLock.readLock();
   private final Lock writeLock = readWriteLock.writeLock();
   private final AtomicInteger index = new AtomicInteger(0);

   private volatile InetSocketAddress[] servers;

   @Override
   public void setServers(Collection<InetSocketAddress> servers) {
      writeLock.lock();
      try {
         this.servers = servers.toArray(new InetSocketAddress[servers.size()]);
         index.set(0);
         if (log.isTraceEnabled()) {
            log.trace("New server list is: " + Arrays.toString(this.servers) + ". Resetting index to 0");
         }
      } finally {
         writeLock.unlock();
      }
   }

   /**
    * Multiple threads might call this method at the same time.
    */
   @Override
   public InetSocketAddress nextServer() {
      readLock.lock();
      try {
         int pos = index.getAndIncrement() % servers.length;
         InetSocketAddress server = servers[pos];
         if (log.isTraceEnabled()) {
            log.trace("Returning server: " + server);
         }
         return server;
      } finally {
         readLock.unlock();
      }
   }
}
