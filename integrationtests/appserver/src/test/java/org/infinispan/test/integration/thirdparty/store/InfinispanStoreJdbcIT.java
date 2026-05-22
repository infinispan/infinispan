package org.infinispan.test.integration.thirdparty.store;

import static org.infinispan.test.integration.thirdparty.DeploymentHelper.addLibrary;
import static org.infinispan.test.integration.thirdparty.DeploymentHelper.isTomcat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit5.container.annotation.ArquillianTest;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Test the Infinispan JDBC CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@ArquillianTest
public class InfinispanStoreJdbcIT {

   private EmbeddedCacheManager cm;

   @Deployment
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addAsResource("jdbc-config.xml");
      addLibrary(war, "org.infinispan:infinispan-core");
      addLibrary(war, "org.infinispan:infinispan-cachestore-jdbc");
      if (isTomcat()) {
         addLibrary(war, "com.h2database:h2");
      }
      return war;
   }

   @AfterEach
   public void cleanUp() {
      if (cm != null)
         cm.stop();
   }

   @Test
   public void testCacheManager() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().defaultCacheName("default");
      ConfigurationBuilder builder = holder.newConfigurationBuilder("default");
      builder.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .table()
            .tableNamePrefix("ISPN")
            .idColumnName("K")
            .idColumnType("VARCHAR(255)")
            .dataColumnName("V")
            .dataColumnType("BLOB")
            .timestampColumnName("T")
            .timestampColumnType("BIGINT")
            .segmentColumnName("S")
            .segmentColumnType("BIGINT")
            .dataSource().jndiUrl(System.getProperty("infinispan.server.integration.data-source"));

      cm = new DefaultCacheManager(holder);

      Cache<String, String> cache = cm.getCache();
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
   }

   @Test
   public void testXmlConfig() throws IOException {
      cm = new DefaultCacheManager("jdbc-config.xml");
      Cache<String, String> cache = cm.getCache("anotherCache");
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
   }
}
