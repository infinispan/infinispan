package org.infinispan.commons.test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.SecureRandom;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@SuppressWarnings("unused")
@MetaInfServices
public class CommonsTestBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // Allow for various threads to determine blocking dynamically - which means the below non blocking predicate
      // will be evaluated each time a blocking operation is found on these threads
      builder.addDynamicThreadPredicate(t ->
            // TestNG may be started on main thread and load this before renaming to testng
            t.getName().startsWith("main") ||
            // The threads our tests run on directly
            t.getName().startsWith("testng") ||
            // These threads are part of AbstractInfinispanTest#testExecutor and fork methods
            t.getName().startsWith("ForkThread"));

      builder.nonBlockingThreadPredicate(threadPredicate -> threadPredicate.or(t -> {
         return BlockHoundHelper.currentThreadRequiresNonBlocking();
      }));

      // SecureRandom reads from a socket
      builder.allowBlockingCallsInside(SecureRandom.class.getName(), "nextBytes");

      // Just assume all the thread pools don't block in our test suite - NOTE rejection policy can still be an issue!
      registerAllPublicMethodsOnClass(builder, ThreadPoolExecutor.class);
      registerAllPublicMethodsOnClass(builder, ScheduledThreadPoolExecutor.class);
      builder.allowBlockingCallsInside(ThreadPoolExecutor.class.getName(), "getTask");
      builder.allowBlockingCallsInside(ThreadPoolExecutor.class.getName(), "processWorkerExit");

      // Let any test suite progress stuff block
      registerAllPublicMethodsOnClass(builder, TestSuiteProgress.class);

      // Allow logging to block in our test suite
      builder.allowBlockingCallsInside(org.apache.logging.log4j.core.Logger.class.getName(), "logMessage");
      builder.allowBlockingCallsInside(java.util.logging.Logger.class.getName(), "log");
   }

   // This is a duplicate of CommonsBlockHoundIntegration - but unfortunately neither can reference each other
   // as commons doesn't rely on commons-test, only the commons test jar
   private static void registerAllPublicMethodsOnClass(BlockHound.Builder builder, Class<?> clazz) {
      Method[] methods = clazz.getMethods();
      for (Method method : methods) {
         if (Modifier.isPublic(method.getModifiers())) {
            builder.allowBlockingCallsInside(clazz.getName(), method.getName());
         }
      }
   }
}
