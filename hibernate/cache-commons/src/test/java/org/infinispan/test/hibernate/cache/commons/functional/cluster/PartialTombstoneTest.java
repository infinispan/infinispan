package org.infinispan.test.hibernate.cache.commons.functional.cluster;

import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.commons.util.Tombstone;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Customer;
import org.infinispan.test.hibernate.cache.commons.util.TestConfigurationHook;

import java.util.Properties;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartialTombstoneTest extends AbstractPartialUpdateTest {

   @Override
   public Class<? extends TestConfigurationHook> getInjectPartialFailure() {
      return InjectPartialFailure.class;
   }

   @Override
   protected boolean doUpdate() throws Exception {
      try {
         withTxSession(localFactory, s -> {
            Customer customer = s.load(Customer.class, 1);
            assertEquals("JBoss", customer.getName());
            customer.setName(customer.getName() + ", a division of Red Hat");
            s.update(customer);
         });
         fail("Expected update to fail");
         return true;
      } catch (CompletionException e) {
         assertExceptionCause(InducedException.class, e);
         return false;
      }
   }

   private static void assertExceptionCause(Class<InducedException> clazz, CompletionException e) {
      Throwable cause = e.getCause();
      while (!clazz.isInstance(cause)) {
         cause = cause.getCause();
      }

      assertTrue("Expected " + clazz + " to be in the stacktrace", clazz.isInstance(cause));
   }

   public static final class InjectPartialFailure extends AbstractPartialUpdateTest.AbstractInjectPartialFailure {

      public InjectPartialFailure(Properties properties) {
         super(properties);
      }

      @Override
      Class<? extends AsyncInterceptor> getFailureInducingInterceptorClass() {
         return FailureInducingInterceptor.class;
      }

   }

   public static class FailureInducingInterceptor extends BaseCustomAsyncInterceptor {

      static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(PartialFutureUpdateTest.FailureInducingInterceptor.class);

      int remoteInvocationCount;

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
         log.tracef("Invoked insert/update: %s", command);

         if (!ctx.isOriginLocal()) {
            remoteInvocationCount++;
            log.tracef("Remote invocation count: %d ", remoteInvocationCount);

            if (command.getKey().toString().endsWith("1")
                  && remoteInvocationCount == 3
                  && command.getFunction() instanceof Tombstone) {
               throw new InducedException("Simulate failure when Tombstone received");
            }
         }

         return super.visitReadWriteKeyCommand(ctx, command);
      }

   }

}
