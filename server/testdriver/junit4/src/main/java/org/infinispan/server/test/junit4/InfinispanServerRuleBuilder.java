package org.infinispan.server.test.junit4;

import java.util.Properties;

import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerRunMode;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

/**
 * Builder for {@link InfinispanServerRule}.
 *
 * @author Diego Lovison &lt;dlovison@redhat.com&gt;
 * @author Dan Berindei
 * @since 10.0
 **/
public class InfinispanServerRuleBuilder {
   private final String configurationFile;
   private final Properties properties;
   private String[] mavenArtifacts;
   private int numServers = 2;
   private ServerRunMode runMode = ServerRunMode.DEFAULT;
   private JavaArchive[] archives;
   private boolean jmx;
   private boolean parallelStartup = true;

   public static InfinispanServerRuleBuilder config(String configurationFile) {
      return new InfinispanServerRuleBuilder(configurationFile);
   }

   private InfinispanServerRuleBuilder(String configurationFile) {
      this.configurationFile = configurationFile;
      this.properties = new Properties(System.getProperties()); // set the defaults
      // also copy org.infinispan properties
      System.getProperties().entrySet().stream().filter(e -> e.getKey().toString().startsWith("org.infinispan")).forEach(e -> properties.put(e.getKey(), e.getValue()));
   }

   public InfinispanServerRuleBuilder mavenArtifacts(String... mavenArtifacts) {
      this.mavenArtifacts = mavenArtifacts;
      return this;
   }

   public InfinispanServerRuleBuilder numServers(int numServers) {
      this.numServers = numServers;
      return this;
   }

   public InfinispanServerRuleBuilder runMode(ServerRunMode serverRunMode) {
      this.runMode = serverRunMode;
      return this;
   }

   /**
    * Deployments
    */
   public InfinispanServerRuleBuilder artifacts(JavaArchive... archives) {
      this.archives = archives;
      return this;
   }

   /**
    * Define a system property
    */
   public InfinispanServerRuleBuilder property(String name, String value) {
      this.properties.setProperty(name, value);
      return this;
   }

   public InfinispanServerRuleBuilder enableJMX() {
      this.jmx = true;
      return this;
   }

   /**
    * If false servers are started individually, waiting until they become available, before subsequent servers are started.
    */
   public InfinispanServerRuleBuilder parallelStartup(boolean parallel) {
      this.parallelStartup = parallel;
      return this;
   }

   public InfinispanServerRule build() {
      InfinispanServerTestConfiguration configuration =
            new InfinispanServerTestConfiguration(configurationFile, numServers, runMode, this.properties, mavenArtifacts,
                                                  archives, jmx, parallelStartup);
      return new InfinispanServerRule(configuration);
   }
}
