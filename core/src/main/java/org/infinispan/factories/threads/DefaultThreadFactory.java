package org.infinispan.factories.threads;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread factory based on JBoss Thread's JBossThreadFactory.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
public class DefaultThreadFactory implements ThreadFactory {

   public static final String DEFAULT_PATTERN = "%c-%n-p%f-t%t";

   private final ThreadGroup threadGroup;
   private final int initialPriority;
   private final String threadNamePattern;

   private final AtomicLong factoryThreadIndexSequence = new AtomicLong(1L);

   private final long factoryIndex;

   private static final AtomicLong globalThreadIndexSequence = new AtomicLong(1L);
   private static final AtomicLong factoryIndexSequence = new AtomicLong(1L);
   private String node;
   private String component;

   /**
    * Construct a new instance.  The access control context of the calling thread will be the one used to create
    * new threads if a security manager is installed.
    *
    * @param threadGroup the thread group to assign threads to by default (may be {@code null})
    * @param initialPriority the initial thread priority, or {@code null} to use the thread group's setting
    * @param threadNamePattern the name pattern string
    */
   public DefaultThreadFactory(ThreadGroup threadGroup, int initialPriority, String threadNamePattern,
         String node, String component) {
      if (threadGroup == null) {
         final SecurityManager sm = System.getSecurityManager();
         threadGroup = sm != null ? sm.getThreadGroup() : Thread.currentThread().getThreadGroup();
      }
      this.threadGroup = threadGroup;
      this.initialPriority = initialPriority;
      factoryIndex = factoryIndexSequence.getAndIncrement();
      if (threadNamePattern == null) {
         threadNamePattern = DefaultThreadFactory.DEFAULT_PATTERN;
      }
      this.threadNamePattern = threadNamePattern;
      this.node = node;
      this.component = component;
   }

   public void setNode(String node) {
      this.node = node;
   }

   public void setComponent(String component) {
      this.component = component;
   }

   public String threadNamePattern() {
      return threadNamePattern;
   }

   public ThreadGroup threadGroup() {
      return threadGroup;
   }

   public int initialPriority() {
      return initialPriority;
   }

   @Override
   public Thread newThread(final Runnable target) {
      return createThread(target);
   }

   private Thread createThread(final Runnable target) {
      final ThreadNameInfo nameInfo = new ThreadNameInfo(globalThreadIndexSequence.getAndIncrement(),
            factoryThreadIndexSequence.getAndIncrement(), factoryIndex, node, component);
      Thread thread = new Thread(threadGroup, target);
      thread.setName(nameInfo.format(thread, threadNamePattern));
      thread.setPriority(initialPriority);
      thread.setDaemon(true);
      return thread;
   }

}
