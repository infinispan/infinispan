package org.infinispan.factories.threads;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.jdkspecific.ThreadCreator;

/**
 * Thread factory based on JBoss Thread's JBossThreadFactory.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
public class DefaultThreadFactory implements ThreadFactory {

   public static final String DEFAULT_PATTERN = "%c-%n-p%f-t%t";

   private final String name;
   private final ThreadGroup threadGroup;
   private final int initialPriority;
   private final String threadNamePattern;

   private final AtomicLong factoryThreadIndexSequence = new AtomicLong(1L);

   private final long factoryIndex;

   private static final AtomicLong globalThreadIndexSequence = new AtomicLong(1L);
   private static final AtomicLong factoryIndexSequence = new AtomicLong(1L);
   private String node;
   private String component;
   private ClassLoader classLoader;

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
      this(null, threadGroup, initialPriority, threadNamePattern, node, component);
   }

   /**
    * Construct a new instance.  The access control context of the calling thread will be the one used to create
    * new threads if a security manager is installed.
    *
    * @param name the name of this thread factory (may be {@code null})
    * @param threadGroup the thread group to assign threads to by default (may be {@code null})
    * @param initialPriority the initial thread priority, or {@code null} to use the thread group's setting
    * @param threadNamePattern the name pattern string
    */
   public DefaultThreadFactory(String name, ThreadGroup threadGroup, int initialPriority, String threadNamePattern,
         String node, String component) {
      this.classLoader = Thread.currentThread().getContextClassLoader();
      this.name = name;
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

   public String getName() {
      return name;
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
      Thread thread = actualThreadCreate(threadGroup, target);
      thread.setName(nameInfo.format(thread, threadNamePattern));
      thread.setPriority(initialPriority);
      thread.setDaemon(true);
      SecurityActions.setContextClassLoader(thread, classLoader);
      return thread;
   }

   private final Thread actualThreadCreate(ThreadGroup threadGroup, Runnable target) {
      return ThreadCreator.createThread(threadGroup, target, true);
   }
}
