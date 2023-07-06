package org.infinispan.server.test.junit4;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.MemcachedTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;
import org.infinispan.server.test.api.TestClientXSiteDriver;
import org.infinispan.server.test.core.TestClient;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 12.0
 **/
public class InfinispanXSiteServerTestMethodRule implements TestRule, TestClientXSiteDriver {
   private final Map<String, TestClient> testClients = new HashMap<>();

   public InfinispanXSiteServerTestMethodRule(InfinispanXSiteServerRule serverRule) {
      Objects.requireNonNull(serverRule, "InfinispanServerRule class Rule is null");
      serverRule.getTestServers().forEach((it) -> this.testClients.put(it.getSiteName(), new TestClient(it)));
   }

   @Override
   public String getMethodName() {
      return testClients.values().iterator().next().getMethodName();
   }

   @Override
   public HotRodTestClientDriver hotrod(String siteName) {
      return testClients.get(siteName).hotrod();
   }

   @Override
   public RestTestClientDriver rest(String siteName) {
      return testClients.get(siteName).rest();
   }

   @Override
   public MemcachedTestClientDriver memcached(String siteName) {
      return testClients.get(siteName).memcached();
   }

   @Override
   public CounterManager getCounterManager(String siteName) {
      return testClients.get(siteName).getCounterManager();
   }

   @Override
   public <K, V> MultimapCacheManager<K, V> getMultimapCacheManager(String siteName) {
      return testClients.get(siteName).getRemoteMultimapCacheManager();
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            try {
               testClients.values().forEach((testClient) -> {
                  testClient.initResources();
                  testClient.setMethodName(description.getTestClass().getSimpleName() + "." + description.getMethodName());
               });
               base.evaluate();
            } finally {
               testClients.values().forEach(TestClient::clearResources);
            }
         }
      };
   }
}
