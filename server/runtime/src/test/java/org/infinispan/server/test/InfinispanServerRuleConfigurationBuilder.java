package org.infinispan.server.test;

import java.util.Properties;

import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * {@link org.junit.ClassRule} constructors should be trivial and complex objects be initialized in the {@link
 * org.junit.rules.TestRule#apply(Statement, Description)} method.
 *
 * @author Diego Lovison &lt;dlovison@redhat.com&gt;
 * @since 10.1
 **/
public class InfinispanServerRuleConfigurationBuilder {

   private final String configurationFile;
   private String[] mavenArtifacts;
   private int numServers = 2;
   private Properties properties = new Properties();
   private ServerRunMode serverRunMode = ServerRunMode.DEFAULT;
   private JavaArchive[] archive;
   private boolean jmx;

   public InfinispanServerRuleConfigurationBuilder(String configurationFile) {
      this.configurationFile = configurationFile;
   }

   /**
    * Extra libs
    *
    * @param mavenArtifacts
    */
   public InfinispanServerRuleConfigurationBuilder mavenArtifacts(String... mavenArtifacts) {
      assert serverRunMode == ServerRunMode.CONTAINER : "Artifacts can only be added when serverRunMode == CONTAINER";
      this.mavenArtifacts = mavenArtifacts;
      return this;
   }

   /**
    * The number of servers in the initial cluster
    *
    * @param numServers
    */
   public InfinispanServerRuleConfigurationBuilder numServers(int numServers) {
      this.numServers = numServers;
      return this;
   }

   /**
    * The {@link ServerRunMode} to use. The default run mode is EMBEDDED unless overridden via the
    * org.infinispan.test.server.driver system property
    *
    * @param serverRunMode
    */
   public InfinispanServerRuleConfigurationBuilder serverRunMode(ServerRunMode serverRunMode) {
      this.serverRunMode = serverRunMode;
      return this;
   }

   /**
    * A system property
    */
   public InfinispanServerRuleConfigurationBuilder property(String name, String value) {
      this.properties.setProperty(name, value);
      return this;
   }

   /**
    * Deployments
    *
    * @param archive
    */
   public InfinispanServerRuleConfigurationBuilder archive(JavaArchive... archive) {
      assert serverRunMode == ServerRunMode.CONTAINER : "Archives can only be added when serverRunMode == CONTAINER";
      this.archive = archive;
      return this;
   }

   public InfinispanServerRuleConfigurationBuilder enableJMX() {
      this.jmx = true;
      return this;
   }

   public InfinispanServerTestConfiguration build() {
      return new InfinispanServerTestConfiguration(configurationFile, numServers, serverRunMode, properties, mavenArtifacts, archive, jmx);
   }
}
