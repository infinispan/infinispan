package org.jboss.arquillian.examples.quickstart.extension;

import java.util.UUID;

import org.infinispan.server.test.InfinispanServerDriver;
import org.infinispan.server.test.InfinispanServerRuleBuilder;
import org.infinispan.server.test.InfinispanServerTestConfiguration;
import org.infinispan.server.test.ServerRunMode;
import org.jboss.arquillian.container.spi.event.container.AfterUnDeploy;
import org.jboss.arquillian.container.spi.event.container.BeforeDeploy;
import org.jboss.arquillian.core.api.annotation.Observes;
import org.jboss.arquillian.test.spi.TestClass;

/**
 * LifecycleExecuter.
 *
 * This code will be execute outside of the Arquillian context
 */
public class LifecycleExecuter {

   private String name = UUID.randomUUID().toString();
   private InfinispanServerDriver driver;

   public void executeBeforeDeploy(@Observes BeforeDeploy event, TestClass testClass) {
      InfinispanServerTestConfiguration configuration = InfinispanServerRuleBuilder
            .config("ispn-config/infinispan.xml").newConfiguration();
      this.driver = ServerRunMode.EMBEDDED.newDriver(configuration);
      this.driver.prepare(this.name);
      this.driver.start(this.name);
   }

   public void executeAfterUnDeploy(@Observes AfterUnDeploy event, TestClass testClass) {
      this.driver.stop(this.name);
   }
}