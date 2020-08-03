package org.infinispan.server.test.junit4;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.server.test.core.TestServer;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Creates a cluster of sites to be used for running multiple tests
 * for Cross-Site replication functionality, It performs the following tasks
 * <ul>
 * <li>It creates a temporary directory using the test name</li>
 * <li>It creates a runtime directory structure for each server in the cluster (data, log, lib)</li>
 * <li>It populates the configuration directory with multiple certificates (ca.pfx, server.pfx, user1.pfx, user2.pfx)</li>
 * </ul>
 *
 * @author Gustavo Lira &lt;gliraesi@redhat.com&gt;
 * @since 12.0
 **/
public class InfinispanXSiteServerRule implements TestRule {
   private final List<TestServer> testServers;

   public InfinispanXSiteServerRule(List<TestServer> testServers) {
      this.testServers = testServers;
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            List<TestServer> servers = new ArrayList<>();
            String testName = description.getTestClass().getName();
            try {
               testServers.forEach((testServer) -> {
                  // Don't manage the server when a test is using the same InfinispanServerRule instance as the parent suite
                  if (!testServer.isDriverInitialized()) {
                     servers.add(testServer);
                     testServer.initServerDriver();
                     testServer.beforeListeners();
                     testServer.getDriver().prepare(testName);
                     testServer.getDriver().start(testName);
                  }
               });
               base.evaluate();
            } finally {
               servers.forEach((testServer) -> {
                  testServer.getDriver().stop(testName);
                  testServer.afterListeners();
               });
            }
         }
      };
   }

   public List<TestServer> getTestServers() {
      return testServers;
   }
}
