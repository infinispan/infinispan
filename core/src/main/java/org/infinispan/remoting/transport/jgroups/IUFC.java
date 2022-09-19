package org.infinispan.remoting.transport.jgroups;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.infinispan.commons.executors.NonBlockingResource;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.UFC;
import org.jgroups.util.Credit;

/**
 * TODO! document this
 */
public class IUFC extends UFC {

   private static final ThreadLocal<Boolean> JGROUPS_THREAD = ThreadLocal.withInitial(() -> Boolean.FALSE);
   static {
      ClassConfigurator.addProtocol((short) 1322, IUFC.class);
   }

   private volatile Executor executor = ForkJoinPool.commonPool();

   public IUFC() {
   }

   @Override
   protected Object handleDownMessage(Message msg, int length) {
      Address dest = msg.getDest();
      assert dest != null;

      InfinispanCredit credits = getCreditFor(dest);
      if (credits == null || credits.nonBlockingDecrementIfEnoughCredits(length)) {
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

   @SuppressWarnings("unchecked")
   @Override
   protected InfinispanCredit createCredit(int initial_credits) {
      return new InfinispanCredit(initial_credits);
   }

   private InfinispanCredit getCreditFor(Address dest) {
      Credit credit = sent.get(dest);
      assert credit instanceof InfinispanCredit;
      return (InfinispanCredit) credit;
   }


   public void setExecutor(Executor executor) {
      this.executor = Objects.requireNonNull(executor);
   }
}
