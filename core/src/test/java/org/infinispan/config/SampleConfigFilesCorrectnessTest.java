/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.config;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
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
   private static final Log log = LogFactory.getLog(SampleConfigFilesCorrectnessTest.class);

   public String configFolder;
   public String configRoot;
   private InMemoryAppender appender;
   private Level oldLevel;

   @BeforeMethod
   public void setUpTest() {
      Logger log4jLogger = Logger.getRootLogger();
      oldLevel = log4jLogger.getLevel();
      log4jLogger.setLevel(Level.WARN);
      appender = new InMemoryAppender();
      log4jLogger.addAppender(appender);
      configFolder = getConfigFolder();
      configRoot = "src" + File.separator + "main" + File.separator
            + "release" + File.separator + "etc" + File.separator + configFolder;
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
         log.tracef("Analysing %s", aConfFile);
         EmbeddedCacheManager dcm = TestCacheManagerFactory.fromXml(getRootFolder() + File.separator + aConfFile);
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

   public void testWarningForMissingQuery() {
      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, true);
      try {
         embeddedCacheManager.getCache();
         assert appender.isFoundUnknownWarning();
         assert appender.unknownWarning().contains("infinispan-query.jar");
      } finally {
         TestingUtil.killCacheManagers(embeddedCacheManager);
      }
   }

   private String[] getConfigFileNames() {
      File file = getRootFolder();
      if (!file.isDirectory()) {
         log.tracef("file.getAbsolutePath() = %s");
      }
      return file.list(new FilenameFilter() {
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

   public String getConfigFolder() {
      return "config-samples";
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
                  "S3_PING could not be substituted",
                  "This might lead to performance problems. Please set your", // TCP and UDP send/recv buffer warnings
                  "stateRetrieval's 'alwaysProvideInMemoryState' attribute is no longer in use",
                  "unable to find an address other than loopback for IP version IPv4"
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
               log.tracef("InMemoryAppender: %s", event.getMessage().toString());
               log.tracef("TOLERABLE_WARNINGS: %s", Arrays.toString(TOLERABLE_WARNINGS));
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
