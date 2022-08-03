package org.infinispan.server.test.junit4;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.TestServer;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.Statement;

/**
 * Creates a cluster of servers to be used for running multiple tests It performs the following tasks:
 * <ul>
 * <li>It creates a temporary directory using the test name</li>
 * <li>It creates a common configuration directory to be shared by all servers</li>
 * <li>It creates a runtime directory structure for each server in the cluster (data, log, lib)</li>
 * <li>It populates the configuration directory with multiple certificates (ca.pfx, server.pfx, user1.pfx, user2.pfx)</li>
 * </ul>
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerRule implements TestRule {
   private static final Log log = LogFactory.getLog(InfinispanServerRule.class);
   private final TestServer testServer;
   protected final List<Consumer<File>> configurationEnhancers = new ArrayList<>();

   public InfinispanServerRule(InfinispanServerTestConfiguration configuration) {
      this.testServer = new TestServer(configuration);
   }

   /**
    * Registers a {@link Consumer} function which populates a server filesystem with additional files.
    * <p>
    * The consumer will be invoked with the server's configuration directory
    */
   public void registerConfigurationEnhancer(Consumer<File> enhancer) {
      configurationEnhancers.add(enhancer);
   }

   public InfinispanServerDriver getServerDriver() {
      return testServer.getDriver();
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            String testName = description.getTestClass().getName();
            RunWith runWith = description.getTestClass().getAnnotation(RunWith.class);
            boolean inSuite = runWith != null && runWith.value() == Suite.class;
            boolean hasXsite = testServer.hasCrossSiteEnabled();
            if (!inSuite && !hasXsite) {
               TestResourceTracker.testStarted(testName);
            }
            // Don't manage the server when a test is using the same InfinispanServerRule instance as the parent suite
            boolean manageServer = !testServer.isDriverInitialized();
            try {
               if (manageServer) {
                  testServer.initServerDriver();
                  testServer.getDriver().prepare(testName);
                  testServer.beforeListeners();

                  configurationEnhancers.forEach(c -> c.accept(testServer.getDriver().getConfDir()));

                  testServer.getDriver().start(testName);
               }
               InfinispanServerRule.this.before(testName);

               base.evaluate();
            } catch (Throwable e) {
               log.error("Problem during the server initialization", e);
               throw e;
            } finally {
               InfinispanServerRule.this.after(testName);
               if (manageServer && testServer.isDriverInitialized()) {
                  testServer.stopServerDriver(testName);
                  testServer.afterListeners();
               }
               if (!inSuite && !hasXsite) {
                  TestResourceTracker.testFinished(testName);
               }
            }
         }
      };
   }

   private void before(String name) {
   }

   private void after(String name) {
   }

   public TestServer getTestServer() {
      return testServer;
   }
}
