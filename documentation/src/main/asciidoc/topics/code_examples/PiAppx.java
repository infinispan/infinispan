public class PiAppx {

   public static void main (String [] arg){
      EmbeddedCacheManager cacheManager = ..
      boolean isCluster = ..

      // The number of darts to shoot at the circle.
      int numPoints = 1_000_000_000;
      int numServers = isCluster ? cacheManager.getMembers().size() : 1;
      int numberPerWorker = numPoints / numServers;

      ClusterExecutor clusterExecutor = cacheManager.executor();
      long start = System.currentTimeMillis();
      // We receive results concurrently from the executor.
      // You must guarantee thread-safety when collecting the results.
      // In this example, we utilize an AtomicLong to collect the results from the executor.
      AtomicLong countCircle = new AtomicLong();

      // Submits the lambda utilizing the executor.
      // The future completes once the computation finishes cluster-wide (exceptionally or not).
      CompletableFuture<Void> fut = clusterExecutor.submitConsumer(m -> {

         // This is the lambda executed remotely across all nodes in the cluster.
         // All the values and methods must be serializable to be transferred across the wire.
         int insideCircleCount = 0;
         for (int i = 0; i < numberPerWorker; i++) {
            double x = Math.random();
            double y = Math.random();
            if (insideCircle(x, y))
               insideCircleCount++;
         }
         return insideCircleCount;
      },
      // This lambda runs on the caller and does not need to be marshallable.
      // It is invoked with the results of each remote call, possibly concurrently.
      // Therefore, collecting the results must guarantee thread-safety.
      (address, count, throwable) -> {
         if (throwable != null) {
            throwable.printStackTrace();
            System.out.println("Address: " + address + " encountered an error: " + throwable);
         } else {
            countCircle.getAndAdd(count);
         }
      });
      fut.whenComplete((v, t) -> {
         // This is invoked after all nodes have responded with a value or exception
         if (t != null) {
            t.printStackTrace();
            System.out.println("Exception encountered while waiting:" + t);
         } else {
            double appxPi = 4.0 * countCircle.get() / numPoints;

            System.out.println("Distributed PI appx is " + appxPi +
                  " using " + numServers + " node(s), completed in " + (System.currentTimeMillis() - start) + " ms");
         }
      });

      // May have to sleep here to keep alive if no user threads left
      fut.join();
   }

   private static boolean insideCircle(double x, double y) {
      return (Math.pow(x - 0.5, 2) + Math.pow(y - 0.5, 2))
            <= Math.pow(0.5, 2);
   }
}
