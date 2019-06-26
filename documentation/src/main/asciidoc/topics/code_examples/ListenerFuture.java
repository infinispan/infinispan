FutureListener futureListener = new FutureListener() {

   public void futureDone(Future future) {
      try {
         future.get();
      } catch (Exception e) {
         // Future did not complete successfully
         System.out.println("Help!");
      }
   }
};
     
cache.putAsync("key", "value").attachListener(futureListener);
