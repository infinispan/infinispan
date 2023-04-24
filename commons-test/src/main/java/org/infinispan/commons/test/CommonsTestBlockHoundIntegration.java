package org.infinispan.commons.test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

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

      // Let any test suite progress stuff block
      registerAllPublicMethodsOnClass(builder, TestSuiteProgress.class);

      builder.markAsBlocking(BlockHoundHelper.class, "blockingConsume", "(Ljava/lang/Object;)V");
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
