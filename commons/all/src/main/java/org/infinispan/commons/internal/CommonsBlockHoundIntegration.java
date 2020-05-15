package org.infinispan.commons.internal;

import java.lang.reflect.Method;

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

      // This should never block as non blocking thread will run the task if pool was full
      builder.disallowBlockingCallsInside(NonBlockingRejectedExecutionHandler.class.getName(), "rejectedExecution");

      // This loads up a file to load the key store and a resource for getContext
      builder.allowBlockingCallsInside(SslContextFactory.class.getName(), "loadKeyStore");
      builder.allowBlockingCallsInside(SslContextFactory.class.getName(), "getContext");

      // This reads in the mime.type file at class initialization
      builder.allowBlockingCallsInside(MediaTypeResolver.class.getName(), "populateFileMap");

      // Loading a service may require opening a file from classpath
      builder.allowBlockingCallsInside(ServiceFinder.class.getName(), "load");
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
}
