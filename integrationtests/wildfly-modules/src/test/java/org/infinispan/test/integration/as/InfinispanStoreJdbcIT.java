package org.infinispan.test.integration.as;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan JDBC CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@RunWith(Arquillian.class)
public class InfinispanStoreJdbcIT {

   private EmbeddedCacheManager cm;

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap.create(WebArchive.class, "jdbc.war")
            .addClass(InfinispanStoreJdbcIT.class)
            .addAsResource("jdbc-config.xml")
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services, org.infinispan.persistence.jdbc:" + Version.getModuleSlot() + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @After
   public void cleanUp() {
      if (cm != null)
         cm.stop();
   }

   @Test
   public void testCacheManager() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.globalJmxStatistics().allowDuplicateDomains(true);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .table()
            .tableNamePrefix("ISPN")
            .idColumnName("K")
            .idColumnType("VARCHAR(255)")
            .dataColumnName("V")
            .dataColumnType("BLOB")
            .timestampColumnName("T")
            .timestampColumnType("BIGINT")
            .dataSource().jndiUrl("java:jboss/datasources/ExampleDS");

      cm = new DefaultCacheManager(gcb.build(), builder.build());

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
