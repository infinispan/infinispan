package org.infinispan.query.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

public final class FailureCounter {

   final AtomicInteger genericFailures = new AtomicInteger(0);
   final AtomicInteger entityFailures = new AtomicInteger(0);

   public int genericFailures() {
      return genericFailures.get();
   }

   public int entityFailures() {
      return entityFailures.get();
   }
}
