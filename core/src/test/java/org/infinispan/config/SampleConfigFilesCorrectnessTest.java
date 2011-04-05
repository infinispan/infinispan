package org.infinispan.config;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;

/**
 * Tests the correctness of the supplied configuration files.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.SampleConfigFilesCorrectnessTest")
public class SampleConfigFilesCorrectnessTest {
   public static final String CONFIG_ROOT = "src/main/resources/config-samples";

   private InMemoryAppender appender;
   private Level oldLevel;

   @BeforeMethod
   public void setUpTest() {
      Logger log4jLogger = Logger.getRootLogger();
      oldLevel = log4jLogger.getLevel();
      log4jLogger.setLevel(Level.WARN);
      appender = new InMemoryAppender();
      log4jLogger.addAppender(appender);
   }

   @AfterMethod
   public void tearDownTest() {
      Logger log4jLogger = Logger.getRootLogger();
      log4jLogger.setLevel(oldLevel);
      log4jLogger.removeAppender(appender);
      appender.close();
   }


   public void testConfigWarnings() throws Exception {
      for (String aConfFile : getConfigFileNames()) {
         System.out.println("Analysing " + aConfFile);
         EmbeddedCacheManager dcm = TestCacheManagerFactory.fromXml(getRootFolder() + "/" + aConfFile, true);
         try {
            dcm.getCache();
            assert !appender.isFoundUnknownWarning() : String.format(
                  "Unknown warning discovered in file %s: %s",
                  aConfFile, appender.unknownWarning());
            for (String cacheName : dcm.getCacheNames()) {
               dcm.getCache(cacheName);
               assert !appender.isFoundUnknownWarning();
            }
         } finally {
            TestingUtil.killCacheManagers(dcm);
         }
      }
   }

   private String[] getConfigFileNames() {
      File file = getRootFolder();
      if (!file.isDirectory()) {
         System.out.println("file.getAbsolutePath() = " + file.getAbsolutePath());
      }
      return file.list(new FilenameFilter() {
         public boolean accept(File dir, String name) {
            return name.endsWith(".xml") && !name.startsWith("jgroups");
         }
      });
   }

   private File getRootFolder() {
      File file = new File(CONFIG_ROOT);
      //this is a hack. If the tests are run from core folder then following if should not be entered.
      //otherwise assume we are runnin
      if (!file.isDirectory()) {
         file = new File("core/" + CONFIG_ROOT);
      }
      return file;
   }


   private static class InMemoryAppender extends AppenderSkeleton {
      String[] TOLERABLE_WARNINGS =
            {
                  "Falling back to DummyTransactionManager from Infinispan",
                  "Please set your max receive buffer in the OS correctly",
                  "receive buffer of socket java.net.MulticastSocket@",
                  "Property ec2.access_key could not be replaced as intended",
                  "Property ec2.access_secret could not be replaced as intended",
                  "Property ec2.bucket could not be replaced as intended",
            };
      String unknownWarning;

      /**
       * As this test runs in parallel with other tests tha also log information, we should disregard other possible
       * warnings from other threads and only consider warnings issues within this test class's test.
       *
       * @see #isExpectedThread()
       */
      private Thread loggerThread = Thread.currentThread();

      protected void append(LoggingEvent event) {
         if (event.getLevel().equals(Level.WARN) && isExpectedThread()) {
            boolean skipPrinting = false;
            for (String knownWarn : TOLERABLE_WARNINGS) {
               if (event.getMessage().toString().indexOf(knownWarn) >= 0)
                  skipPrinting = true;
            }

            if (!skipPrinting) {
               unknownWarning = event.getMessage().toString();
               System.out.println("InMemoryAppender ****** " + event.getMessage().toString());
               System.out.println("TOLERABLE_WARNINGS: " + Arrays.toString(TOLERABLE_WARNINGS));
            }
         }
      }

      public boolean requiresLayout() {
         return false;
      }

      public void close() {
         //do nothing
      }

      public boolean isFoundUnknownWarning() {
         return unknownWarning != null;
      }

      public String unknownWarning() {
         return unknownWarning;
      }

      public boolean isExpectedThread() {
         return loggerThread.equals(Thread.currentThread());
      }
   }
}
