package org.infinispan.query.affinity;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.LockObtainFailedException;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.exception.ErrorContext;
import org.hibernate.search.exception.ErrorHandler;
import org.hibernate.search.exception.impl.LogErrorHandler;
import org.infinispan.query.backend.WrappingErrorHandler;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Unveils {@link LockObtainFailedException} from the Hibernate Search backend and presents it
 *
 * @since 9.0
 */
public class AffinityErrorHandler extends WrappingErrorHandler {

   private static final Log log = LogFactory.getLog(AffinityErrorHandler.class, Log.class);

   private OperationFailedHandler operationFailedHandler;

   public AffinityErrorHandler(ErrorHandler handler) {
      super(handler);
   }

   public AffinityErrorHandler() {
      super(new LogErrorHandler());
   }

   public void initialize(OperationFailedHandler operationFailedHandler) {
      if (log.isDebugEnabled()) log.debugf("Initializing with %s", operationFailedHandler);
      this.operationFailedHandler = operationFailedHandler;
   }

   @Override
   protected void errorOccurred(ErrorContext context) {
      if (operationFailedHandler != null) {
         Throwable throwable = context.getThrowable();
         log.errorOccurredApplyingChanges(throwable);
         List<LuceneWork> failed = extractFailedOperations(context);
         operationFailedHandler.operationsFailed(failed, throwable);
      }
   }

   private List<LuceneWork> extractFailedOperations(ErrorContext context) {
      List<LuceneWork> failingOperations = context.getFailingOperations();
      LuceneWork operationAtFault = context.getOperationAtFault();
      List<LuceneWork> failed = new ArrayList<>(failingOperations);
      failed.add(operationAtFault);
      return failed;
   }

   @Override
   protected void exceptionOccurred(String errorMsg, Throwable exception) {
      log.error(errorMsg, exception);
   }
}
