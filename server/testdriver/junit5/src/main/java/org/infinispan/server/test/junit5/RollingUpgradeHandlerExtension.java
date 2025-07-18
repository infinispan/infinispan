package org.infinispan.server.test.junit5;

import java.net.InetAddress;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.JmxTestClient;
import org.infinispan.server.test.api.MemcachedTestClientDriver;
import org.infinispan.server.test.api.RespTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.infinispan.server.test.core.rollingupgrade.CombinedInfinispanServerDriver;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RollingUpgradeHandlerExtension extends AbstractServerExtension implements BeforeEachCallback,
      AfterEachCallback, TestClientDriver {
   private final RollingUpgradeConfigurationBuilder configurationBuilder;
   private RollingUpgradeHandler handler;
   private TestServer testServer;
   private TestClient testClient;

   // TODO: do we just accept the config ?
   public RollingUpgradeHandlerExtension(RollingUpgradeConfigurationBuilder configurationBuilder) {
      this.configurationBuilder = configurationBuilder;
   }

   public static RollingUpgradeHandlerExtension from(Class<?> caller, InfinispanServerExtensionBuilder iseb, String fromVersion, String toVersion) {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(caller.getName(), fromVersion, toVersion);
      InfinispanServerTestConfiguration configuration = iseb.createServerTestConfiguration();

      if (configuration.isDefaultFile()) {
         builder.useDefaultServerConfiguration(configuration.configurationFile());
      } else {
         builder.useCustomServerConfiguration(configuration.configurationFile());
      }
      builder.nodeCount(configuration.numServers())
            .addMavenArtifacts(configuration.mavenArtifacts())
            .addArtifacts(configuration.archives());

      configuration.properties().forEach((k, v) -> {
         if (k instanceof String ks && v instanceof String vs) {
            builder.addProperty(ks, vs);
         }
      });
      configuration.listeners().forEach(builder::addListener);

      return new RollingUpgradeHandlerExtension(builder);
   }

   @Override
   protected void onTestsComplete(ExtensionContext extensionContext) {
      if (handler != null) {
         testServer.afterListeners();
         if (extensionContext.getExecutionException().isPresent()) {
            handler.exceptionEncountered(extensionContext.getExecutionException().get());
         } else {
            handler.complete();
         }
      }
   }

   @Override
   protected void onTestsStart(ExtensionContext extensionContext) throws Exception {
      // Only start the handler when the first test of the suite is encountered. This prevents multiple handlers starting
      // at the beginning of the entire JUnit run and instead only start a new suite after the previous was shutdown:q
      if (handler == null && !isSuiteClass(extensionContext)) {
         handler = RollingUpgradeHandler.runUntilMixed(configurationBuilder.build());
         // Config is only used with from.. is that okay??
         testServer = new TestServer(handler.getFromConfig(), new CombinedInfinispanServerDriver(handler.getFromDriver(), handler.getToDriver()));
      }
   }

   @Override
   public void beforeEach(ExtensionContext context) {
      this.testClient = new TestClient(testServer);
      startTestClient(context, testClient);
   }

   @Override
   public void afterEach(ExtensionContext extensionContext) {
      testClient.clearResources();
   }

   @Override
   public String addScript(RemoteCacheManager remoteCacheManager, String script) {
      return testClient.addScript(remoteCacheManager, script);
   }

   @Override
   public boolean isContainerized() {
      return true;
   }

   @Override
   public HotRodTestClientDriver hotrod() {
      return testClient.hotrod();
   }

   @Override
   public RestTestClientDriver rest() {
      return testClient.rest();
   }

   @Override
   public RespTestClientDriver resp() {
      return testClient.resp();
   }

   @Override
   public MemcachedTestClientDriver memcached() {
      return testClient.memcached();
   }

   @Override
   public JmxTestClient jmx() {
      return testClient.jmx();
   }

   @Override
   public String getMethodName() {
      return testClient.getMethodName();
   }

   @Override
   public String getMethodName(String qualifier) {
      return testClient.getMethodName(qualifier);
   }

   @Override
   public CounterManager getCounterManager() {
      return testClient.getCounterManager();
   }

   @Override
   public InetAddress getServerAddress(int offset) {
      return testClient.getServerDriver().getServerAddress(offset);
   }
}
