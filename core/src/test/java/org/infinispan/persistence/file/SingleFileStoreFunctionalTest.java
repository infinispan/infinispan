package org.infinispan.persistence.file;

import static org.testng.AssertJUnit.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Single file cache store functional test.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = {"unit", "smoke"}, testName = "persistence.file.SingleFileStoreFunctionalTest")
public class SingleFileStoreFunctionalTest extends BaseStoreFunctionalTest {

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(tmpDirectory);
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      persistence
         .addSingleFileStore()
         .location(tmpDirectory)
         .preload(preload);
      return persistence;
   }

   public void testParsingEmptyElement() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <persistence passivation=\"false\"> \n" +
            "         <file-store shared=\"false\" preload=\"true\"/> \n" +
            "      </persistence>\n" +
            "   </local-cache>\n" +
            "</cache-container>");
      ConfigurationBuilderHolder holder = new ParserRegistry().parse(config);
      List<StoreConfiguration> storeConfigs = holder.getDefaultConfigurationBuilder().build().persistence().stores();
      assertEquals(1, storeConfigs.size());
      SingleFileStoreConfiguration fileStoreConfig = (SingleFileStoreConfiguration) storeConfigs.get(0);
      assertEquals("Infinispan-SingleFileStore", fileStoreConfig.location());
      assertEquals(-1, fileStoreConfig.maxEntries());
   }

   public void testParsingElement() throws Exception {
      String config = TestingUtil.wrapXMLWithoutSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <persistence passivation=\"false\"> \n" +
            "         <file-store path=\"other-location\" max-entries=\"100\" shared=\"false\" preload=\"true\" fragmentation-factor=\"0.75\"/> \n" +
            "      </persistence>\n" +
            "   </local-cache>\n" +
            "</cache-container>");
      InputStream is = new ByteArrayInputStream(config.getBytes());
      ConfigurationBuilderHolder holder = new ParserRegistry().parse(config);
      List<StoreConfiguration> storeConfigs = holder.getDefaultConfigurationBuilder().build().persistence().stores();
      assertEquals(1, storeConfigs.size());
      SingleFileStoreConfiguration fileStoreConfig = (SingleFileStoreConfiguration) storeConfigs.get(0);
      assertEquals("other-location", fileStoreConfig.location());
      assertEquals(100, fileStoreConfig.maxEntries());
      assertEquals(0.75f, fileStoreConfig.fragmentationFactor(), 0f);
      Util.recursiveFileRemove("other-location");
   }

}
