package org.infinispan.commons.internal;

import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.logging.log4j.spi.AbstractLogger;
import org.infinispan.commons.dataconversion.MediaTypeResolver;
import org.infinispan.commons.executors.NonBlockingResource;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.commons.util.concurrent.NonBlockingRejectedExecutionHandler;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class CommonsBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      builder.nonBlockingThreadPredicate(current -> current.or(thread -> thread.getThreadGroup() instanceof NonBlockingResource));

      // Pretend to block without the overhead of calling Thread.yield()
      builder.markAsBlocking(BlockHoundUtil.class, "pretendBlock", "()V");

      // This should never block as non blocking thread will run the task if pool was full
      builder.disallowBlockingCallsInside(NonBlockingRejectedExecutionHandler.class.getName(), "rejectedExecution");

      // This loads up a file to load the key store and a resource for getContext
      builder.allowBlockingCallsInside(SslContextFactory.class.getName(), "loadKeyStore");
      builder.allowBlockingCallsInside(SslContextFactory.class.getName(), "getContext");

      // This reads in the mime.type file at class initialization
      builder.allowBlockingCallsInside(MediaTypeResolver.class.getName(), "populateFileMap");

      // Loading a service may require opening a file from classpath
      builder.allowBlockingCallsInside(ServiceFinder.class.getName(), "load");

      // BoundedLocalCache is unfortunately package private
      builder.allowBlockingCallsInside("com.github.benmanes.caffeine.cache.BoundedLocalCache", "performCleanUp");

      handleJREClasses(builder);

      log4j(builder);
   }

   // Register all methods of a given class to allow for blocking - NOTE that if these methods invoke passed in code,
   // such as a Runnable/Callable, this should not be used!
   public static void allowPublicMethodsToBlock(BlockHound.Builder builder, Class<?> clazz) {
      allowMethodsToBlock(builder, clazz, true);
   }

   public static void allowMethodsToBlock(BlockHound.Builder builder, Class<?> clazz, boolean publicOnly) {
      Method[] methods = publicOnly ? clazz.getMethods() : clazz.getDeclaredMethods();
      for (Method method : methods) {
         builder.allowBlockingCallsInside(clazz.getName(), method.getName());
      }
   }

   private static void handleJREClasses(BlockHound.Builder builder) {
      // The runWorker method can block waiting for a new task to be submitted - this is okay
      builder.allowBlockingCallsInside(ForkJoinPool.class.getName(), "runWorker");
      // The scan method is where the task is actually ran
      builder.disallowBlockingCallsInside(ForkJoinPool.class.getName(), "scan");

      // SecureRandom reads from a socket
      builder.allowBlockingCallsInside(SecureRandom.class.getName(), "nextBytes");

      // Just assume all the thread pools don't block - NOTE rejection policy can still be an issue!
      allowMethodsToBlock(builder, ThreadPoolExecutor.class, true);
      allowMethodsToBlock(builder, ScheduledThreadPoolExecutor.class, true);
      builder.allowBlockingCallsInside(ThreadPoolExecutor.class.getName(), "getTask");
      builder.allowBlockingCallsInside(ThreadPoolExecutor.class.getName(), "processWorkerExit");

      // Allow logging to block
      builder.allowBlockingCallsInside(java.util.logging.Logger.class.getName(), "log");
   }

   private static void log4j(BlockHound.Builder builder) {
      try {
         Class.forName("org.apache.logging.log4j.spi.AbstractLogger");
         builder.allowBlockingCallsInside(AbstractLogger.class.getName(), "logMessage");
      } catch (ClassNotFoundException e) {
         // Ignore if no AbstractLogger
      }

      builder.allowBlockingCallsInside("org.apache.logging.log4j.core.Logger", "logMessage");
   }
}
