package ${package};

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;

/**
 * Sample client application code. For more examples please see our client documentation
 * (https://infinispan.org/docs/stable/titles/hotrod_java/hotrod_java.html).
 * <p>
 * In order to run this application, it's necessary for a Infinispan server to be executing on your localhost with the
 * HotRod endpoint available on port 11222. The easiest way to achieve this, is by utilising our server docker image:
 * <p>
 * <code>docker run -p 11222:11222 -e USER="user" -e PASS="pass" infinispan/server</code>
 */
public class Application {

   public void propertyConfiguration() {
      System.out.println("\n\n1.  Demonstrating basic usage of Infinispan Client using 'hotrod-client.properties' configuration.");
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager()) {
         // Retrieve the cache defined by `infinispan.client.hotrod.cache.my-cache.template_name=`
         RemoteCache<String, String> cache = remoteCacheManager.getCache("my-cache");
         helloWord(cache);
      }
   }

   public void programmaticConfiguration() {
      System.out.println("\n\n2.  Demonstrating basic usage of the Infinispan Client using programmatic configuration.");
      ConfigurationBuilder cb = new ConfigurationBuilder();
      // Configure security
      cb.security()
            .authentication()
            .username("user")
            .password("pass");
      // Configure the server(s) to connect to
      cb.addServer()
            .host("127.0.0.1")
            .port(11222);
      // Configure the caches to create on first access
      cb.remoteCache("my-cache")
            .templateName("org.infinispan.DIST_SYNC");

      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(cb.build())) {
         RemoteCache<String, String> cache = remoteCacheManager.getCache("my-cache");
         helloWord(cache);
      }
   }

   private void helloWord(RemoteCache<String, String> cache) {
      System.out.println("  Storing value 'World' under key 'Hello'");
      String oldValue = cache.put("Hello", "World");
      System.out.printf("  Done.  Saw old value as '%s'\n", oldValue);

      System.out.println("  Replacing 'World' with 'Mars'.");
      boolean worked = cache.replace("Hello", "World", "Mars");
      System.out.printf("  Successful? %s\n", worked);

      assert oldValue == null;
      assert worked;
   }

   public void asyncOperations() {
      System.out.println("\n\n3.  Demonstrating asynchronous operations - where writes can be done in a non-blocking fashion.");
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager()) {

         // Retrieve the cache defined by `infinispan.client.hotrod.cache.wine-cache.template_name=`
         RemoteCache<String, Integer> cache = remoteCacheManager.getCache("wine-cache");
         System.out.println("  Put #1");
         CompletableFuture<Integer> f1 = cache.putAsync("Pinot Noir", 300);
         System.out.println("  Put #1");
         CompletableFuture<Integer> f2 = cache.putAsync("Merlot", 120);
         System.out.println("  Put #1");
         CompletableFuture<Integer> f3 = cache.putAsync("Chardonnay", 180);

         // now poll the futures to make sure any remote calls have completed!
         for (CompletableFuture<Integer> f : Arrays.asList(f1, f2, f3)) {
            try {
               System.out.println("  Checking future... ");
               f.get();
            } catch (Exception e) {
               throw new RuntimeException("Operation failed!", e);
            }
         }
         System.out.println("  Everything stored!");
      }
   }

   public void registeringListeners() throws InterruptedException {
      System.out.println("\n\n4.  Demonstrating use of listeners.");
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager()) {
         // Retrieve the cache defined by `infinispan.client.hotrod.cache.listener-cache.template_name=`
         RemoteCache<Integer, String> cache = remoteCacheManager.getCache("listener-cache");

         System.out.println("  Attaching listener");
         cache.addClientListener(new MyListener());

         System.out.println("  Put #1");
         cache.put(1, "One");
         System.out.println("  Remove #1");
         cache.remove(1);
         System.out.println("  Put #2");
         cache.put(2, "Two");
         System.out.println("  Put #3");
         cache.put(3, "Three");
         System.out.println("  Update #3");
         cache.put(3, "3");
         Thread.sleep(1000);
      }
   }

   public static void main(String[] args) throws Exception {
      System.out.println("\n\n\n   ********************************  \n\n\n");
      System.out.println("Hello.  This is a sample application making use of Infinispan.");
      Application a = new Application();
      a.propertyConfiguration();
      a.programmaticConfiguration();
      a.asyncOperations();
      a.registeringListeners();
      System.out.println("Sample complete.");
      System.out.println("\n\n\n   ********************************  \n\n\n");
   }

   @ClientListener
   @SuppressWarnings("unused")
   public static class MyListener {

      @ClientCacheEntryCreated
      public void handleCreatedEvent(ClientCacheEntryCreatedEvent<Integer> e) {
         System.out.printf("Thread %s has created an entry in the cache under key %s!\n",
               Thread.currentThread().getName(), e.getKey());
      }

      @ClientCacheEntryModified
      public void handleModifiedEvent(ClientCacheEntryModifiedEvent<Integer> e) {
         System.out.printf("Thread %s has modified an entry in the cache under key %s!\n",
               Thread.currentThread().getName(), e.getKey());
      }

      @ClientCacheEntryRemoved
      public void handleRemovedEvent(ClientCacheEntryRemovedEvent<Integer> e) {
         System.out.printf("Thread %s has removed an entry in the cache under key %s!\n",
               Thread.currentThread().getName(), e.getKey());
      }
   }
}
