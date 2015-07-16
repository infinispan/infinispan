package org.infinispan.configuration;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests the correctness of the supplied configuration files.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.SampleConfigFilesCorrectnessTest")
public class SampleConfigFilesCorrectnessTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(SampleConfigFilesCorrectnessTest.class);

   public String configRoot;
   private InMemoryAppender appender;

   @BeforeClass
   public void installInMemoryAppender() {
      final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      final Configuration config = ctx.getConfiguration();
      config.addAppender(new InMemoryAppender());
   }

   @BeforeMethod
   public void setUpTest() {
      final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      final Configuration config = ctx.getConfiguration();
      appender = (InMemoryAppender) config.getAppender("InMemory");
      appender.enable(Thread.currentThread());
      ctx.updateLoggers();
      configRoot = "../distribution/src/main/release/common/configs/config-samples".replace('/', File.separatorChar);
   }

   @AfterMethod
   public void tearDownTest() {
      appender.disable();

   }


   public void testConfigWarnings() throws Exception {
      for (String aConfFile : getConfigFileNames()) {
         log.tracef("Analysing %s", aConfFile);
         EmbeddedCacheManager dcm = null;
         try {
            dcm = TestCacheManagerFactory.fromXml(getRootFolder() + File.separator + aConfFile, false, true);
            dcm.getCache();
            assert !appender.isFoundUnknownWarning() : String.format(
                  "Unknown warning discovered in file %s: %s",
                  aConfFile, appender.unknownWarning());
            for (String cacheName : dcm.getCacheNames()) {
               dcm.getCache(cacheName);
               assert !appender.isFoundUnknownWarning();
            }
         } catch (Exception e) {
            throw new Exception("Could not parse '"+aConfFile+"'", e);
         } finally {
            TestingUtil.killCacheManagers(dcm);
         }
      }
   }

   private String[] getConfigFileNames() {
      File file = getRootFolder();
      if (!file.isDirectory()) {
         log.tracef("file.getAbsolutePath() = %s");
      }
      return file.list(new FilenameFilter() {
         @Override
         public boolean accept(File dir, String name) {
            // Exclude JGroups config files as well as all EC2 configurations (as these won't have proper credentials set)
            return name.endsWith(".xml") && !name.startsWith("jgroups") && !name.contains("ec2");
         }
      });
   }

   private File getRootFolder() {
      File file = new File(new File(configRoot).getAbsolutePath());
      //this is a hack. If the tests are run from core folder then following if should not be entered.
      //otherwise assume we are runnin
      if (!file.isDirectory()) {
         file = new File("core/" + configRoot);
      }
      return file;
   }

   private static class InMemoryAppender extends AbstractAppender {

      /** The serialVersionUID */
      private static final long serialVersionUID = 1L;

      protected InMemoryAppender() {
         super("InMemory", null, PatternLayout.createDefaultLayout());
      }

      String[] TOLERABLE_WARNINGS =
            {
                  "Falling back to DummyTransactionManager from Infinispan",
                  "Please set your max receive buffer in the OS correctly",
                  "receive buffer of socket java.net.MulticastSocket@",
                  "Property ec2.access_key could not be replaced as intended",
                  "Property ec2.access_secret could not be replaced as intended",
                  "Property ec2.bucket could not be replaced as intended",
                  "S3_PING could not be substituted",
                  "This might lead to performance problems. Please set your", // TCP and UDP send/recv buffer warnings
                  "stateRetrieval's 'alwaysProvideInMemoryState' attribute is no longer in use",
                  "unable to find an address other than loopback for IP version IPv4",
                  "Partition handling doesn't work for replicated caches, it will be ignored"
            };
      String unknownWarning = null;

      /**
       * As this test runs in parallel with other tests that also log information, we should disregard other possible
       * warnings from other threads and only consider warnings issues within this test class's test.
       *
       * @see #isExpectedThread()
       */
      private Thread loggerThread = null;

      public void disable() {
         loggerThread = null;
      }

      public void enable(Thread thread) {
         loggerThread = thread;
      }

      public boolean isFoundUnknownWarning() {
         return unknownWarning != null;
      }

      public String unknownWarning() {
         return unknownWarning;
      }

      public boolean isExpectedThread() {
         return loggerThread != null && loggerThread.equals(Thread.currentThread());
      }

      @Override
      public void append(LogEvent event) {
         if (event.getLevel().equals(Level.WARN) && isExpectedThread()) {
            boolean skipPrinting = false;
            for (String knownWarn : TOLERABLE_WARNINGS) {
               if (event.getMessage().toString().indexOf(knownWarn) >= 0)
                  skipPrinting = true;
            }

            if (!skipPrinting) {
               unknownWarning = event.getMessage().toString();
            }
         }
      }
   }
}
