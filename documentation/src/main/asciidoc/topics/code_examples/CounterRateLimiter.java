// 5 request per minute
CounterConfiguration configuration = CounterConfiguration.builder(CounterType.BOUNDED_STRONG)
      .upperBound(5)
      .lifespan(60000)
      .build();
counterManager.defineCounter("rate_limiter", configuration);
StrongCounter counter = counterManager.getStrongCounter("rate_limiter");

// on each operation, invoke
try {
   counter.incrementAndGet().get();
   // continue with operation
} catch (InterruptedException e) {
   Thread.currentThread().interrupt();
} catch (ExecutionException e) {
   if (e.getCause() instanceof CounterOutOfBoundsException) {
      // maximum rate. discard operation
      return;
   } else {
      // unexpected error, handling property
   }
}
