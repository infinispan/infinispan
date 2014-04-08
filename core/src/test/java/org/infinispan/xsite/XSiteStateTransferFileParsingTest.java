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

import static org.infinispan.configuration.cache.XSiteStateTransferConfigurationBuilder.*;
import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.testng.AssertJUnit.*;

/**
 * It tests if the cross site replication configuration is correctly parsed and validated.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "xsite.XSiteStateTransferFileParsingTest")
public class XSiteStateTransferFileParsingTest extends SingleCacheManagerTest {

   private static final TakeOfflineConfiguration DEFAULT_TAKE_OFFLINE = new TakeOfflineConfiguration(0, 0);
   private static final XSiteStateTransferConfiguration DEFAULT_STATE_TRANSFER =
         new XSiteStateTransferConfiguration(DEFAULT_CHUNK_SIZE, DEFAULT_TIMEOUT, DEFAULT_MAX_RETRIES, DEFAULT_WAIT_TIME);

   private static final String FILE_NAME = "configs/xsite/xsite-state-transfer-test.xml";
   private static final String XML_FORMAT = INFINISPAN_START_TAG +
         "<jgroups>\n" +
         "  <stack-file name=\"udp\" path=\"jgroups-udp.xml\"/>\n" +
         "</jgroups>\n" +
         "<cache-container default-cache=\"default\">\n" +
         "  <transport cluster=\"infinispan-cluster\" lock-timeout=\"50000\" stack=\"udp\" node-name=\"Jalapeno\" machine=\"m1\"\n" +
         "                   rack=\"r1\" site=\"LON\"/>\n" +
         "  <replicated-cache name=\"default\">\n" +
         "     <backups>\n" +
         "        <backup site=\"NYC\" strategy=\"SYNC\" failure-policy=\"WARN\" timeout=\"12003\">\n" +
         "           <state-transfer chunk-size=\"10\" timeout=\"%s\" max-retries=\"30\" wait-time=\"%s\" />\n" +
         "        </backup>\n" +
         "     </backups>\n" +
         "     <backup-for remote-cache=\"someCache\" remote-site=\"SFO\"/>\n" +
         "  </replicated-cache>\n" +
         "</cache-container>\n" +
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
      assertTrue(dcc.sites().allBackups().contains(create(98, 7654, 321, 101)));
      assertEquals("someCache", dcc.sites().backupFor().remoteCache());
      assertEquals("SFO", dcc.sites().backupFor().remoteSite());
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "Timeout must be higher or equals than 1 \\(one\\).")
   public void testNegativeTimeout() throws IOException {
      testInvalidConfiguration(String.format(XML_FORMAT, -1, DEFAULT_WAIT_TIME));
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "Timeout must be higher or equals than 1 \\(one\\).")
   public void testZeroTimeout() throws IOException {
      testInvalidConfiguration(String.format(XML_FORMAT, 0, DEFAULT_WAIT_TIME));
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "Waiting time between retries must be higher or equals than 1 \\(one\\).")
   public void testNegativeWaitTime() throws IOException {
      testInvalidConfiguration(String.format(XML_FORMAT, DEFAULT_TIMEOUT, -1));
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "Waiting time between retries must be higher or equals than 1 \\(one\\).")
   public void testZeroWaitTime() throws IOException {
      testInvalidConfiguration(String.format(XML_FORMAT, DEFAULT_TIMEOUT, 0));
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
      assertTrue(dcc.sites().allBackups().contains(create(123, 4567, 890, 1011)));
      assertEquals("someCache", dcc.sites().backupFor().remoteCache());
      assertEquals("SFO", dcc.sites().backupFor().remoteSite());
   }

   private static BackupConfiguration create(int chunkSize, long timeout, int maxRetries, long waitingTimeBetweenRetries) {
      XSiteStateTransferConfiguration stateTransferConfiguration = new XSiteStateTransferConfiguration(chunkSize, timeout,
                                                                                                       maxRetries,
                                                                                                       waitingTimeBetweenRetries);
      return new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC, 12003, BackupFailurePolicy.WARN,
                                     null, false, DEFAULT_TAKE_OFFLINE, stateTransferConfiguration, true);
   }

   private static BackupConfiguration createDefault() {
      return new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC, 12003, BackupFailurePolicy.WARN,
                                     null, false, DEFAULT_TAKE_OFFLINE, DEFAULT_STATE_TRANSFER, true);
   }

}
