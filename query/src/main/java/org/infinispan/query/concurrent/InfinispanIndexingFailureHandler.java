package org.infinispan.query.concurrent;

import org.hibernate.search.engine.reporting.EntityIndexingFailureContext;
import org.hibernate.search.engine.reporting.FailureContext;
import org.hibernate.search.engine.reporting.FailureHandler;
import org.hibernate.search.engine.reporting.impl.LogFailureHandler;

public class InfinispanIndexingFailureHandler implements FailureHandler {

   private final FailureCounter failureCounter = new FailureCounter();
   private final FailureHandler baseFailureHandler = new LogFailureHandler();

   @Override
   public void handle(FailureContext context) {
      failureCounter.genericFailures.incrementAndGet();
      baseFailureHandler.handle(context);
   }

   @Override
   public void handle(EntityIndexingFailureContext context) {
      failureCounter.entityFailures.incrementAndGet();
      baseFailureHandler.handle(context);
   }

   public FailureCounter failureCounter() {
      return failureCounter;
   }
}
