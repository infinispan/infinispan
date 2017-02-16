package org.infinispan.query.affinity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.exception.ErrorContext;
import org.hibernate.search.exception.ErrorHandler;
import org.hibernate.search.exception.SearchException;
import org.infinispan.lucene.InvalidLockException;
import org.infinispan.lucene.impl.DirectoryExtensions;
import org.infinispan.query.backend.WrappingErrorHandler;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.LogFactory;

/**
 * Handles errors occurred in the {@link AffinityIndexManager}.
 * @since 9.0
 */
public class AffinityErrorHandler extends WrappingErrorHandler {

   private static final Log log = LogFactory.getLog(AffinityErrorHandler.class, Log.class);

   private RpcManager rpcManager;
   private ExecutorService asyncExecutor;

   public AffinityErrorHandler(ErrorHandler handler) {
      super(handler);
   }

   public void initialize(RpcManager rpcManager, ExecutorService asyncExecutor) {
      this.rpcManager = rpcManager;
      this.asyncExecutor = asyncExecutor;
   }

   @Override
   protected boolean errorOccurred(ErrorContext context) {
      if (!this.shouldHandle(context)) {
         return false;
      }

      AffinityIndexManager affinityIndexManager = (AffinityIndexManager) context.getIndexManager();
      ShardAddress localShardAddress = affinityIndexManager.getLocalShardAddress();

      List<LuceneWork> failed = this.extractFailedWorks(context);

      this.clearLockIfNeeded(affinityIndexManager);

      log.debugf("Retrying operations %s at %s", failed, affinityIndexManager.getLocalShardAddress());
      CompletableFuture.supplyAsync(() -> {
         affinityIndexManager.performOperations(failed, null, true, true);
         return null;
      }, asyncExecutor).whenComplete((aVoid, error) -> {
         if (error == null) {
            log.debugf("Operation %s completed at %s", failed, localShardAddress);
         } else {
            log.errorf(error, "Error reapplying operation %s at %s", failed, localShardAddress);
         }
      });

      return true;
   }

   private void clearLockIfNeeded(AffinityIndexManager affinityIndexManager) {
      List<Address> members = rpcManager.getMembers();
      Address lockHolder = affinityIndexManager.getLockHolder();
      log.debugf("Current members are %s, lock holder is %s", members, lockHolder);
      if (lockHolder != null && !members.contains(lockHolder)) {
         Directory directory = affinityIndexManager.getDirectoryProvider().getDirectory();
         log.debugf("Forcing clear of index lock %s", affinityIndexManager.getIndexName());
         ((DirectoryExtensions) directory).forceUnlock(IndexWriter.WRITE_LOCK_NAME);
      }
   }

   private List<LuceneWork> extractFailedWorks(ErrorContext errorContext) {
      List<LuceneWork> failingOperations = errorContext.getFailingOperations();
      LuceneWork operationAtFault = errorContext.getOperationAtFault();
      List<LuceneWork> failed = new ArrayList<>(failingOperations);
      failed.add(operationAtFault);
      return failed;
   }

   private boolean shouldHandle(ErrorContext context) {
      if (!(context.getIndexManager() instanceof AffinityIndexManager)) {
         return false;
      }
      Throwable throwable = context.getThrowable();
      return throwable instanceof LockObtainFailedException || throwable instanceof SearchException &&
            throwable.getCause() instanceof InvalidLockException;
   }
}
