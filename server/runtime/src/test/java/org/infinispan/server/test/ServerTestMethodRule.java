package org.infinispan.server.test;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerTestMethodRule implements TestRule {
   private final ServerTestRule serverTestRule;
   private String methodName;
   private List<RemoteCache> remoteCaches = new ArrayList<>();

   public ServerTestMethodRule(ServerTestRule serverTestRule) {
      assert serverTestRule != null;
      this.serverTestRule = serverTestRule;
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            before();
            try {
               ServerTestMethodConfiguration config = description.getAnnotation(ServerTestMethodConfiguration.class);
               methodName = description.getMethodName();
               base.evaluate();
            } finally {
               after();
            }
         }
      };
   }

   private void before() {
   }

   private void after() {
      remoteCaches.forEach(remoteCache -> serverTestRule.hotRodClient().administration().removeCache(remoteCache.getName()));
      remoteCaches.clear();
   }

   public <K, V> RemoteCache<K, V> getHotRodCache(CacheMode mode) {
      return serverTestRule.hotRodClient().administration().getOrCreateCache(methodName, "org.infinispan." + mode.name());
   }
}
