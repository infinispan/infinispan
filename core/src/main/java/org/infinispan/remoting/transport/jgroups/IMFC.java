package org.infinispan.remoting.transport.jgroups;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.infinispan.commons.executors.NonBlockingResource;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.MFC;
import org.jgroups.util.CreditMap;

/**
 * TODO! document this
 */
public class IMFC extends MFC {

   private static final ThreadLocal<Boolean> JGROUPS_THREAD = ThreadLocal.withInitial(() -> Boolean.FALSE);
   static {
      ClassConfigurator.addProtocol((short) 1323, IMFC.class);
   }

   private volatile Executor executor = ForkJoinPool.commonPool();

   public IMFC() {
   }

   @Override
   protected Object handleDownMessage(Message msg, int length) {
      InfinispanCreditsMap credits = creditsMap();
      if (credits.nonBlockingDecrementIfEnoughCredits(length)) {
         return down_prot.down(msg);
      }

      //not enough credits! decide if block thread or not
      if (Thread.currentThread().getThreadGroup() instanceof NonBlockingResource || JGROUPS_THREAD.get()) {
         credits.queue(() -> super.handleDownMessage(msg, length), executor);
         return null;
      }

      // thread can be blocked, use the original method
      return super.handleDownMessage(msg, length);
   }

   @Override
   public Object up(Message msg) {
      JGROUPS_THREAD.set(Boolean.TRUE);
      try {
         return super.up(msg);
      } finally {
         JGROUPS_THREAD.set(Boolean.FALSE);
      }
   }

   @Override
   protected CreditMap createCreditMap(long max_creds) {
      return new InfinispanCreditsMap(max_credits);
   }

   private InfinispanCreditsMap creditsMap() {
      assert credits instanceof InfinispanCreditsMap;
      return (InfinispanCreditsMap) credits;
   }


   public void setExecutor(Executor executor) {
      this.executor = Objects.requireNonNull(executor);
   }
}
