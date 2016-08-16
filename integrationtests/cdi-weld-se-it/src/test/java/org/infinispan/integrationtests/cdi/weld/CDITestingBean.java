package org.infinispan.integrationtests.cdi.weld;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.infinispan.Cache;

/**
 * CDI bean for testing.
 *
 * @author Sebastian Laskawiec
 */
@ApplicationScoped
public class CDITestingBean {

   @Inject
   private Cache<String, String> cache;

   public void putValueInCache(String key, String value) {
      cache.put(key, value);
   }

   public String getValueFromCache(String key) {
      return cache.get(key);
   }
}
