package org.infinispan.server.test.jupiter;

import java.net.InetAddress;

import org.infinispan.client.hotrod.RemoteCacheContainer;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.test.api.CliTestDriver;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.JmxTestClient;
import org.infinispan.server.test.api.MemcachedTestClientDriver;
import org.infinispan.server.test.api.RespTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.infinispan.server.test.core.TestSetupUtil;
import org.infinispan.server.test.core.compatibility.Compatibility;
import org.infinispan.server.test.core.rollingupgrade.CombinedInfinispanServerDriver;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeVersion;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.shrinkwrap.api.Archive;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RollingUpgradeHandlerExtension extends AbstractServerExtension implements BeforeEachCallback,
      AfterEachCallback, TestClientDriver {
   private static final Log log = LogFactory.getLog(RollingUpgradeHandlerExtension.class);

   private final RollingUpgradeConfigurationBuilder configurationBuilder;
   private RollingUpgradeHandler handler;
   private TestServer testServer;
   private TestClient testClient;
   // These suites run the entire functional test suite against a single shared mixed-version cluster.
   // If that cluster stops responding the remaining tests would otherwise all fail with identical
   // timeouts. Once a failure is confirmed to be caused by an unresponsive cluster we set this flag so
   // subsequent tests are skipped instead, keeping the first genuine failure while removing the noise.
   private boolean clusterUnresponsive;

   public RollingUpgradeHandlerExtension(RollingUpgradeConfigurationBuilder configurationBuilder) {
      this.configurationBuilder = configurationBuilder;
   }

   public static RollingUpgradeHandlerExtension from(Class<?> caller, InfinispanServerExtensionBuilder iseb, RollingUpgradeVersion fromVersion, RollingUpgradeVersion toVersion) {
      return new RollingUpgradeHandlerExtension(convertBuilder(caller, iseb, fromVersion, toVersion));
   }

   public static RollingUpgradeConfigurationBuilder convertBuilder(Class<?> caller, InfinispanServerExtensionBuilder iseb, RollingUpgradeVersion fromVersion, RollingUpgradeVersion toVersion) {
      return convertBuilder(caller.getName(), iseb, fromVersion, toVersion);
   }

   public static RollingUpgradeConfigurationBuilder convertBuilder(String name, InfinispanServerExtensionBuilder iseb, RollingUpgradeVersion fromVersion, RollingUpgradeVersion toVersion) {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(name, fromVersion, toVersion);
      InfinispanServerTestConfiguration configuration = iseb.createServerTestConfiguration();

      if (configuration.isDefaultFile()) {
         builder.useDefaultServerConfiguration(configuration.configurationFile());
      } else {
         builder.useCustomServerConfiguration(configuration.configurationFile());
      }
      String[] artifacts = configuration.mavenArtifacts();
      if (artifacts != null) {
         builder.addMavenArtifacts(artifacts);
      }
      Archive<?>[] javaArchives = configuration.archives();
      if (javaArchives != null) {
         builder.addArchives(javaArchives);
      }
      builder.nodeCount(configuration.numServers());

      configuration.properties().forEach((k, v) -> {
         if (k instanceof String ks && v instanceof String vs) {
            builder.addProperty(ks, vs);
         }
      });
      configuration.listeners().forEach(builder::addListener);
      return builder;
   }

   @Override
   protected void onTestsComplete(ExtensionContext extensionContext) {
      if (handler != null) {
         testServer.afterListeners();
         if (extensionContext.getExecutionException().isPresent()) {
            handler.exceptionEncountered(extensionContext.getExecutionException().get());
         } else {
            handler.completeUpgrade(true);
         }
      }
   }

   @Override
   protected void onTestsStart(ExtensionContext extensionContext) {
      // Only start the handler when the first test of the suite is encountered. This prevents multiple handlers starting
      // at the beginning of the entire JUnit run and instead only start a new suite after the previous was shutdown:q
      if (handler == null && !isSuiteClass(extensionContext)) {
         handler = RollingUpgradeHandler.runUntilMixed(configurationBuilder.build());
         // Config is only used with from... is that okay??
         testServer = new TestServer(handler.getFromConfig(), new CombinedInfinispanServerDriver(handler.getFromDriver(), handler.getToDriver()));
      }
   }

   @Override
   public void beforeEach(ExtensionContext context) {
      Assumptions.assumeFalse(Compatibility.INSTANCE.isCompatibilitySkip(handler.getConfiguration(), context.getRequiredTestClass().getName(), context.getRequiredTestMethod().getName()));
      // A previous test already confirmed the shared cluster is unresponsive; skip the rest of the
      // suite rather than piling up identical timeout failures.
      Assumptions.assumeFalse(clusterUnresponsive,
            "Skipping - rolling upgrade cluster is not responding, see earlier failure");
      this.testClient = new TestClient(testServer);
      startTestClient(context, testClient);
   }

   @Override
   public void afterEach(ExtensionContext extensionContext) {
      try {
         // When a test fails for a real reason (not an assumption based skip) probe the shared cluster.
         // If it is no longer responding we treat the failure as the start of an outage and mark the
         // cluster so subsequent tests are skipped instead of repeating the same timeout failure. A
         // genuine assertion failure leaves the cluster healthy, so the probe passes and later tests
         // still run normally.
         if (!clusterUnresponsive && isRealFailure(extensionContext) && !isClusterResponsive()) {
            clusterUnresponsive = true;
            log.warnf("Rolling upgrade cluster '%s' is not responding after a test failure; " +
                  "remaining suite tests will be skipped", handler.getConfiguration().name());
         }
      } finally {
         testClient.clearResources();
      }
   }

   private static boolean isRealFailure(ExtensionContext context) {
      return context.getExecutionException()
            .filter(t -> !TestSetupUtil.isAssumptionViolated(t))
            .isPresent();
   }

   /**
    * Probes the shared mixed-version cluster using the same predicate the handler uses to gate each
    * upgrade step (a synchronous Hot Rod get plus a server count check). Any exception, including the
    * client socket timeout, is treated as the cluster being unresponsive.
    */
   private boolean isClusterResponsive() {
      try {
         return handler.getConfiguration().isValidServerState().test(handler);
      } catch (Exception e) {
         log.debugf(e, "Rolling upgrade cluster health probe failed");
         return false;
      }
   }

   @Override
   public String addScript(RemoteCacheContainer remoteCacheManager, String script) {
      return testClient.addScript(remoteCacheManager, script);
   }

   @Override
   public boolean isContainerized() {
      return true;
   }

   @Override
   public CliTestDriver cli() {
      return testClient.cli();
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
