package org.infinispan.tx.lockreordering;

import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

/**
* @author Mircea Markus
* @since 5.1
*/
public class StresserThread extends Thread {

   private static Log log = LogFactory.getLog(StresserThread.class);

   private static final int NUM_TRANSACTIONS = 250;
   static final OperationsPerformer PUT_PERFORMER = new OperationsPerformer() {
      @Override
      public void perform(List keys, Object value, Cache cache) {
         for (Object k : keys) {
            cache.put(k, value);
         }
      }
   };
   static final OperationsPerformer REMOVE_PERFORMER = new OperationsPerformer() {
      @Override
      public void perform(List keys, Object value, Cache cache) {
         for (Object o : keys) {
            cache.remove(o);
         }
      }
   };
   static final OperationsPerformer PUT_ALL_PERFORMER = new OperationsPerformer() {
      @Override
      public void perform(List keys, Object value, Cache cache) {
         Map toAdd = new LinkedHashMap();
         for (Object o : keys) {
            toAdd.put(o, value);
         }
         cache.putAll(toAdd);
      }
   };
   static final OperationsPerformer MIXED_OPS_PERFORMER = new OperationsPerformer() {
      @Override
      public void perform(List keys, Object value, Cache cache) {
         final Random r = new Random();
         for (Object o : keys) {
            final int op = r.nextInt(3);
            switch (op) {
               case 0: {
                  cache.put(o, value);
                  break;
               }
               case 1: {
                  cache.remove(o);
                  break;
               }
               case 2: {
                  cache.putAll(Collections.singletonMap(o, value));
                  break;
               }
            }
         }
      }
   };

   public final Cache cache;
   public final List keys;
   public final String value;
   public final OperationsPerformer op;

   volatile boolean error = false;
   private final CyclicBarrier beforeCommit;


   public StresserThread(Cache cache, List keys, String value, OperationsPerformer op, CyclicBarrier beforeCommit, String threadName) {
      super(threadName);
      this.cache = cache;
      this.keys = keys;
      this.value = value;
      this.op = op;
      this.beforeCommit = beforeCommit;
   }

   @Override
   public void run() {
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      for (int i = 0; i < NUM_TRANSACTIONS; i++) {
         try {
            tm.begin();
            op.perform(keys, value, cache);
            beforeCommit.await(10, TimeUnit.SECONDS);
            tm.commit();
         } catch (Throwable t) {
            log.error("Exception:", t);
            error = true;
            return;
         }
      }
   }

   public boolean isError() {
      return error;
   }

   public interface OperationsPerformer {
      void perform(List keys, Object value, Cache cache);
   }
}
