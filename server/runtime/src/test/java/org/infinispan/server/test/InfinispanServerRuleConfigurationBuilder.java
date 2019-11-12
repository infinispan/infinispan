package org.infinispan.server.test;

import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * {@link org.junit.ClassRule} constructors should be trivial and complex objects be initialized in the
 * {@link org.junit.rules.TestRule#apply(Statement, Description)} method.
 *
 * @author Diego Lovison &lt;dlovison@redhat.com&gt;
 * @since 10.0
 **/
public class InfinispanServerRuleConfigurationBuilder {

   private final String configurationFile;
   private String[] mavenArtifacts;
   private Integer numServers;
   private ServerRunMode serverRunMode;
   private JavaArchive[] archive;

   public InfinispanServerRuleConfigurationBuilder(String configurationFile) {
      this.configurationFile = configurationFile;
   }

   public InfinispanServerRuleConfigurationBuilder mavenArtifacts(String... mavenArtifacts) {
      this.mavenArtifacts = mavenArtifacts;
      return this;
   }

   public InfinispanServerRuleConfigurationBuilder numServers(Integer numServers) {
      this.numServers = numServers;
      return this;
   }

   public InfinispanServerRuleConfigurationBuilder serverRunMode(ServerRunMode serverRunMode) {
      this.serverRunMode = serverRunMode;
      return this;
   }

   public InfinispanServerRuleConfigurationBuilder archive(JavaArchive... archive) {
      this.archive = archive;
      return this;
   }

   public InfinispanServerTestConfiguration build() {
      InfinispanServerTestConfiguration configuration = new InfinispanServerTestConfiguration(this.configurationFile);
      if (this.numServers != null) {
         configuration.numServers(this.numServers);
      }
      if (this.serverRunMode != null) {
         configuration.runMode(this.serverRunMode);
      }
      if (this.mavenArtifacts != null) {
         configuration.mavenArtifacts(this.mavenArtifacts);
      }
      if (this.archive != null) {
         configuration.artifacts(this.archive);
      }
      return configuration;
   }
}
