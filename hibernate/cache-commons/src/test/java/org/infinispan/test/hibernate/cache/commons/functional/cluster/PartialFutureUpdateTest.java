package org.infinispan.test.hibernate.cache.commons.functional.cluster;

import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.hibernate.cache.commons.util.FutureUpdate;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Customer;
import org.infinispan.test.hibernate.cache.commons.util.TestConfigurationHook;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class PartialFutureUpdateTest extends AbstractPartialUpdateTest {

   @Override
   public Class<? extends TestConfigurationHook> getInjectPartialFailure() {
      return InjectPartialFailure.class;
   }

   @Override
   protected boolean doUpdate() throws Exception {
      withTxSession(localFactory, s -> {
         Customer customer = s.load(Customer.class, 1);
         assertEquals("JBoss", customer.getName());
         customer.setName(customer.getName() + ", a division of Red Hat");
         s.update(customer);
      });
      return true;
   }

   public static final class InjectPartialFailure extends AbstractInjectPartialFailure {

      public InjectPartialFailure(Properties properties) {
         super(properties);
      }

      @Override
      Class<? extends AsyncInterceptor> getFailureInducingInterceptorClass() {
         return FailureInducingInterceptor.class;
      }

   }

   public static class FailureInducingInterceptor extends BaseCustomAsyncInterceptor {

      static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(FailureInducingInterceptor.class);

      int remoteInvocationCount;

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
         log.tracef("Invoked insert/update: %s", command);

         if (!ctx.isOriginLocal()) {
            remoteInvocationCount++;
            log.tracef("Remote invocation count: %d ", remoteInvocationCount);

            if (command.getKey().toString().endsWith("#1")
                  && remoteInvocationCount == 4
                  && command.getFunction() instanceof FutureUpdate) {
               throw new AbstractPartialUpdateTest.InducedException("Simulate failure when FutureUpdate received");
            }
         }

         return super.visitReadWriteKeyCommand(ctx, command);
      }

   }

}
