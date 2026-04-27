package test.org.infinispan.spring.starter.remote.schema;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/greetings")
public class GreetingController {

   public static final String CACHE_NAME = "greetings";

   private final RemoteCacheManager cacheManager;

   public GreetingController(RemoteCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @PutMapping("/{id}")
   public Greeting put(@PathVariable String id, @RequestBody Greeting greeting) {
      RemoteCache<String, Greeting> cache = cacheManager.getCache(CACHE_NAME);
      cache.put(id, greeting);
      return greeting;
   }

   @GetMapping("/{id}")
   public ResponseEntity<Greeting> get(@PathVariable String id) {
      RemoteCache<String, Greeting> cache = cacheManager.getCache(CACHE_NAME);
      Greeting greeting = cache.get(id);
      if (greeting == null) {
         return ResponseEntity.notFound().build();
      }
      return ResponseEntity.ok(greeting);
   }
}
