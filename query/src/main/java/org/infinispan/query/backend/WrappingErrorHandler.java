package org.infinispan.query.backend;

import org.hibernate.search.exception.ErrorContext;
import org.hibernate.search.exception.ErrorHandler;

/**
 * Wraps another Hibernate Search {@link ErrorHandler} allowing extra processing of the backend error.
 * @since 9.0
 */
public abstract class WrappingErrorHandler implements ErrorHandler {

   private final ErrorHandler errorHandler;

   public WrappingErrorHandler(ErrorHandler errorHandler) {
      this.errorHandler = errorHandler;
   }

   @Override
   public void handle(ErrorContext context) {
      boolean handled = errorOccurred(context);
      if (!handled) {
         errorHandler.handle(context);
      }
   }

   @Override
   public void handleException(String errorMsg, Throwable exception) {
      errorHandler.handleException(errorMsg, exception);
   }

   protected abstract boolean errorOccurred(ErrorContext context);

   public ErrorHandler unwrap() {
      return errorHandler;
   }
}
