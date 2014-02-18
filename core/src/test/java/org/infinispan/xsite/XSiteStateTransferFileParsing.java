package org.infinispan.xsite;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.infinispan.configuration.cache.XSiteStateTransferConfigurationBuilder.DEFAULT_CHUNK_SIZE;
import static org.infinispan.configuration.cache.XSiteStateTransferConfigurationBuilder.DEFAULT_TIMEOUT;
import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.testng.AssertJUnit.*;

/**
 * It tests if the cross site replication configuration is correctly parsed and validated.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "xsite.XSiteStateTransferFileParsing")
public class XSiteStateTransferFileParsing extends SingleCacheManagerTest {

   private static final TakeOfflineConfiguration DEFAULT_TAKE_OFFLINE = new TakeOfflineConfiguration(0, 0);
   private static final XSiteStateTransferConfiguration DEFAULT_STATE_TRANSFER =
         new XSiteStateTransferConfiguration(DEFAULT_CHUNK_SIZE, DEFAULT_TIMEOUT);

   private static final String FILE_NAME = "configs/xsite/xsite-state-transfer-test.xml";
   private static final String XML_FORMAT = INFINISPAN_START_TAG +
         "<global>\n" +
         "   <site local=\"LON\"/>\n" +
         "   <transport clusterName=\"infinispan-cluster\" distributedSyncTimeout=\"50000\" nodeName=\"Jalapeno\" machineId=\"m1\"\n" +
         "              rackId=\"r1\" siteId=\"s1\">\n" +
         "      <properties>\n" +
         "         <property name=\"configurationFile\" value=\"jgroups-udp.xml\"/>\n" +
         "       </properties>\n" +
         "   </transport>\n" +
         "</global>\n" +
         "<default>\n" +
         "   <sites>\n" +
         "      <backups>\n" +
         "         <backup site=\"NYC\" strategy=\"SYNC\" backupFailurePolicy=\"IGNORE\" timeout=\"12003\">\n" +
         "            <stateTransfer chunkSize=\"10\" timeout=\"%s\" />\n" +
         "         </backup>\n" +
         "      </backups>\n" +
         "      <backupFor remoteCache=\"someCache\" remoteSite=\"SFO\"/>\n" +
         "   </sites>" +
         "</default>\n" +
         INFINISPAN_END_TAG;

   public void testDefaultCache() {
      Configuration dcc = cacheManager.getDefaultCacheConfiguration();
      assertEquals(1, dcc.sites().allBackups().size());
      testDefault(dcc);
   }

   public void testInheritor() {
      Configuration dcc = cacheManager.getCacheConfiguration("inheritor");
      assertEquals(1, dcc.sites().allBackups().size());
      testDefault(dcc);
   }

   public void testNoStateTransfer() {
      Configuration dcc = cacheManager.getCacheConfiguration("noStateTransfer");
      assertEquals(1, dcc.sites().allBackups().size());
      assertTrue(dcc.sites().allBackups().contains(createDefault()));
      assertNull(dcc.sites().backupFor().remoteSite());
      assertNull(dcc.sites().backupFor().remoteCache());
   }

   public void testStateTransferDifferentConfig() {
      Configuration dcc = cacheManager.getCacheConfiguration("stateTransferDifferentConfiguration");
      assertEquals(1, dcc.sites().allBackups().size());
      assertTrue(dcc.sites().allBackups().contains(create(98, 7654)));
      assertEquals("someCache", dcc.sites().backupFor().remoteCache());
      assertEquals("SFO", dcc.sites().backupFor().remoteSite());
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "Timeout must be higher or equals than 1 \\(one\\).")
   public void testNegativeTimeout() throws IOException {
      testInvalidConfiguration(String.format(XML_FORMAT, -1));
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "Timeout must be higher or equals than 1 \\(one\\).")
   public void testZeroTimeout() throws IOException {
      testInvalidConfiguration(String.format(XML_FORMAT, 0));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml(FILE_NAME);
   }

   private void testInvalidConfiguration(String xmlConfiguration) throws IOException {
      EmbeddedCacheManager invalidCacheManager = null;
      try {
         log.infof("Creating cache manager with %s", xmlConfiguration);
         invalidCacheManager = TestCacheManagerFactory.fromStream(new ByteArrayInputStream(xmlConfiguration.getBytes()));
      } finally {
         if (invalidCacheManager != null) {
            invalidCacheManager.stop();
         }
      }
   }

   private void testDefault(Configuration dcc) {
      assertTrue(dcc.sites().allBackups().contains(create(123, 4567)));
      assertEquals("someCache", dcc.sites().backupFor().remoteCache());
      assertEquals("SFO", dcc.sites().backupFor().remoteSite());
   }

   private static BackupConfiguration create(int chunkSize, long timeout) {
      XSiteStateTransferConfiguration stateTransferConfiguration = new XSiteStateTransferConfiguration(chunkSize, timeout);
      return new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC, 12003, BackupFailurePolicy.WARN,
                                     null, false, DEFAULT_TAKE_OFFLINE, stateTransferConfiguration, true);
   }

   private static BackupConfiguration createDefault() {
      return new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC, 12003, BackupFailurePolicy.WARN,
                                     null, false, DEFAULT_TAKE_OFFLINE, DEFAULT_STATE_TRANSFER, true);
   }

}
