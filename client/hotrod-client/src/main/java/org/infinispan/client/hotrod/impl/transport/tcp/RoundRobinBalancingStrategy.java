package org.infinispan.client.hotrod.impl.transport.tcp;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * // TODO: Document this
 *
 * todo - this can be called from several threads, synchronize!
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
   private final AtomicInteger index = new AtomicInteger();

   private InetSocketAddress[] servers;

   @Override
   public void setServers(Collection<InetSocketAddress> servers) {
      writeLock.lock();
      try {
         this.servers = servers.toArray(new InetSocketAddress[servers.size()]);
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
         int pos = index.incrementAndGet() % servers.length;
         InetSocketAddress server = servers[pos];
         if (log.isTraceEnabled()) {
            log.trace("Retuning server: " + server);
         }
         return server;
      } finally {
         readLock.unlock();
      }
   }
}
