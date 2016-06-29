package org.infinispan.query.backend;

import org.hibernate.search.exception.ErrorContext;
import org.hibernate.search.exception.ErrorHandler;

/**
 * Wraps another Hibernate Search {@link ErrorHandler} allowing extra processing of the backend error.
 *
 * @since 9.0
 */
public abstract class WrappingErrorHandler implements ErrorHandler {

   private final ErrorHandler errorHandler;

   public WrappingErrorHandler(ErrorHandler errorHandler) {
      this.errorHandler = errorHandler;
   }

   @Override
   public void handle(ErrorContext context) {
      errorOccurred(context);
      errorHandler.handle(context);
   }

   @Override
   public void handleException(String errorMsg, Throwable exception) {
      exceptionOccurred(errorMsg, exception);
      errorHandler.handleException(errorMsg, exception);
   }

   protected abstract void errorOccurred(ErrorContext context);

   protected abstract void exceptionOccurred(String errorMsg, Throwable exception);

   public ErrorHandler unwrap() {
      return errorHandler;
   }
}
