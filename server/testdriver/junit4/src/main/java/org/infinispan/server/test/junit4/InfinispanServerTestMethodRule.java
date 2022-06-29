package org.infinispan.server.test.junit4;

import net.spy.memcached.MemcachedClient;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;
import org.infinispan.server.test.core.TestClient;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerTestMethodRule implements TestRule, TestClientDriver {
   private TestClient testClient;

   public InfinispanServerTestMethodRule(InfinispanServerRule infinispanServerRule) {
      Objects.requireNonNull(infinispanServerRule, "InfinispanServerRule class Rule is null");
      this.testClient = new TestClient(infinispanServerRule.getTestServer());
   }

   @Override
   public String getMethodName() {
      return testClient.getMethodName();
   }

   @Override
   public HotRodTestClientDriver hotrod() {
      return testClient.hotrod();
   }

   public HotRodTestClientDriver hotrod(Consumer<ConfigurationBuilder> additionalConfigurations) {
      return testClient.hotrod(additionalConfigurations);
   }

   @Override
   public RestTestClientDriver rest() {
      return testClient.rest();
   }

   @Override
   public CounterManager getCounterManager() {
      return testClient.getCounterManager();
   }

   // Used for internal test
   public MemcachedClient getMemcachedClient() {
      return testClient.getMemcachedClient();
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            testClient.initResources();
            try {
               testClient.setMethodName(description.getTestClass().getSimpleName() + "." + description.getMethodName());
               base.evaluate();
            } finally {
               testClient.clearResources();
            }
         }
      };
   }

   public String addScript(RemoteCacheManager remoteCacheManager, String script) {
      return testClient.addScript(remoteCacheManager, script);
   }

   public RestClient newRestClient(RestClientConfigurationBuilder restClientConfigurationBuilder) {
      return testClient.newRestClient(restClientConfigurationBuilder);
   }
}
