package org.infinispan.client.hotrod.impl.transaction.recovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class RecoveryIterator {

   private static final Log log = LogFactory.getLog(RecoveryIterator.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final Xid[] NOTHING = new Xid[0];
   private final Set<Xid> uniqueFilter = Collections.synchronizedSet(new HashSet<>());
   private final BlockingDeque<Xid> inDoubtTransactions = new LinkedBlockingDeque<>();
   private final CompletableFuture<Void> remoteRequest;

   RecoveryIterator(Collection<Xid> localTransactions, CompletableFuture<Collection<Xid>> remoteRequest) {
      add(localTransactions);
      this.remoteRequest = remoteRequest.thenAccept(this::add);
   }

   public Xid[] next() {
      if (inDoubtTransactions.isEmpty()) {
         if (trace) {
            log.trace("RecoveryIterator.next() = []");
         }
         return NOTHING;
      }

      Collection<Xid> txs = new ArrayList<>(inDoubtTransactions.size());
      inDoubtTransactions.drainTo(txs);
      if (trace) {
         log.tracef("RecoveryIterator.next() = %s", txs);
      }
      return txs.toArray(NOTHING);
   }

   public void finish(long timeout) {
      try {
         remoteRequest.get(timeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
         e.printStackTrace();
      }
   }

   private void add(Collection<Xid> transactions) {
      for (Xid xid : transactions) {
         if (uniqueFilter.add(xid)) {
            if (trace) {
               log.tracef("RecoveryIterator new xid=%s", xid);
            }
            inDoubtTransactions.add(xid);
         }
      }
   }
}
