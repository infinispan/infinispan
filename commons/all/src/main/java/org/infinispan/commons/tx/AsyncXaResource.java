package org.infinispan.commons.tx;

import java.util.concurrent.CompletionStage;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Non-blocking {@link XAResource}.
 *
 * @since 14.0
 */
public interface AsyncXaResource {

   /**
    * @return A {@link CompletionStage} which is completed with the result of {@link XAResource#end(Xid, int)}.
    * @see XAResource#end(Xid, int)
    */
   CompletionStage<Void> asyncEnd(XidImpl xid, int flags);

   /**
    * @return A {@link CompletionStage} which is completed with the result of {@link XAResource#prepare(Xid)}.
    * @see XAResource#prepare(Xid)
    */
   CompletionStage<Integer> asyncPrepare(XidImpl xid);

   /**
    * @return A {@link CompletionStage} which is completed with the result of {@link XAResource#commit(Xid, boolean)}
    * @see XAResource#commit(Xid, boolean)
    */
   CompletionStage<Void> asyncCommit(XidImpl xid, boolean onePhase);

   /**
    * @return A {@link CompletionStage} which is completed with the result of {@link XAResource#rollback(Xid)}
    * @see XAResource#rollback(Xid)
    */
   CompletionStage<Void> asyncRollback(XidImpl xid);

}
