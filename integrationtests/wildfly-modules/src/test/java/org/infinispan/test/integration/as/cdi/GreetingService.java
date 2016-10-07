package org.infinispan.test.integration.as.cdi;

import javax.cache.annotation.CacheResult;

/**
 * <p>This is the Greeting Service class.</p>
 *
 * <p>Each call to the {@link GreetingService#greet(String)} method will be cached in the greeting-cache (in this case
 * the {@linkplain javax.cache.annotation.CacheKey CacheKey} will be the name). If this method has been already called
 * with the same name the cached value will be returned and this method will not be called.</p>
 *
 * @author Kevin Pollet <pollet.kevin@gmail.com> (C) 2011
 * @see CacheResult
 */
public class GreetingService {

   @CacheResult(cacheName = "greeting-cache")
   public java.lang.String greet(String name) {
      return "Hello " + name + " :)";
   }

}
