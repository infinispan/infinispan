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
package org.infinispan.config.parsing;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.FileCacheStoreConfiguration;
import org.infinispan.configuration.cache.LoadersConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "unit", testName = "config.parsing.Jbc2InfinispanTransformerTest")
public class Jbc2InfinispanTransformerTest extends AbstractInfinispanTest {
   public static final String XSLT_FILE = "xslt/jbc3x2infinispan4x.xslt";
   private static final String BASE_DIR = "configs/jbosscache3x";
   ConfigFilesConvertor convertor = new ConfigFilesConvertor();

   /**
    * Transforms and tests the transformation of a complex file.
    */
   public void testAllFile() throws Exception {
      ClassLoader existingCl = Thread.currentThread().getContextClassLoader();
      try {
         ClassLoader delegatingCl = new TestClassLoader(existingCl);
         Thread.currentThread().setContextClassLoader(delegatingCl);
         String fileName = getFileName("all.xml");
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         convertor.parse(fileName, baos, XSLT_FILE, Thread.currentThread().getContextClassLoader());

         log.trace("Output file is:\n" + baos.toString());
         withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(new ByteArrayInputStream(baos.toByteArray()))) {
            @Override
            public void call() {
               Configuration defaultConfig = cm.getDefaultCacheConfiguration();
               GlobalConfiguration globalConfig = cm.getCacheManagerConfiguration();

               assert defaultConfig.locking().isolationLevel().equals(IsolationLevel.READ_COMMITTED);
               assert defaultConfig.locking().lockAcquisitionTimeout() == 234000;
               assert defaultConfig.locking().concurrencyLevel() == 510;
               assert defaultConfig.transaction().transactionManagerLookup().getClass().equals(GenericTransactionManagerLookup.class);
               assert !defaultConfig.transaction().syncCommitPhase();
               assert defaultConfig.transaction().syncRollbackPhase();
               assert defaultConfig.jmxStatistics().enabled();
               assert globalConfig.shutdown().hookBehavior().equals(ShutdownHookBehavior.DONT_REGISTER);
               assert globalConfig.asyncListenerExecutor().properties().get("maxThreads").equals("123");
               assert globalConfig.asyncListenerExecutor().properties().get("queueSize").equals("1020000");
               assert !defaultConfig.invocationBatching().enabled();
               assert globalConfig.serialization().marshaller().getClass().equals(VersionAwareMarshaller.class);
               assert defaultConfig.storeAsBinary().enabled();

               assert globalConfig.transport().clusterName().equals("JBossCache-cluster");
               assert defaultConfig.clustering().cacheMode().equals(CacheMode.INVALIDATION_SYNC);
               assert defaultConfig.clustering().stateTransfer().timeout() == 2120000;

               assert defaultConfig.clustering().sync().replTimeout() == 22220000;
               assert defaultConfig.eviction().strategy() == EvictionStrategy.LRU;
               assert defaultConfig.eviction().maxEntries() == 5001;
               assert defaultConfig.expiration().maxIdle() == 1001 : "Received " + defaultConfig.expiration().lifespan();
               assert defaultConfig.expiration().wakeUpInterval() == 50015;

               Configuration regionOne = cm.getCacheConfiguration("/org/jboss/data1");
               assert regionOne != null;
               assert regionOne.eviction().strategy() == EvictionStrategy.LRU;
               assert regionOne.expiration().maxIdle() == 2002;
               assert regionOne.expiration().wakeUpInterval() == 50015;

               Configuration regionTwo = cm.getCacheConfiguration("/org/jboss/data2");
               assert regionTwo != null;
               assert regionTwo.eviction().strategy() == EvictionStrategy.FIFO;
               assert regionTwo.eviction().maxEntries() == 3003;
               assert regionTwo.expiration().wakeUpInterval() == 50015;

               LoadersConfiguration loaders = defaultConfig.loaders();
               assert loaders.passivation();
               assert loaders.shared();

               assert loaders.cacheLoaders().size() == 1;

               FileCacheStoreConfiguration fcsc = (FileCacheStoreConfiguration) loaders.cacheLoaders().get(0);
               assert fcsc.async().enabled();
               assert fcsc.fetchPersistentState();
               assert fcsc.ignoreModifications();
               assert fcsc.purgeOnStartup();
               assert fcsc.singletonStore().enabled();
            }
         });
      } finally {
         Thread.currentThread().setContextClassLoader(existingCl);
      }
   }

   /**
    * Just to make sure that the transformer won't transforming exiting configs.
    */
   public void testCanTransformExistingFiles() throws Exception {
      ClassLoader existingCl = Thread.currentThread().getContextClassLoader();
      try {
         ClassLoader delegatingCl = new TestClassLoader(existingCl);
         Thread.currentThread().setContextClassLoader(delegatingCl);
         String[] testFiles = {"buddy-replication.xml", "cacheloader-enabled.xml", "eviction-enabled.xml",
                               "external-jgroups-file.xml",
                               "invalidation-async.xml", "total-replication.xml"};
         for (String name : testFiles) {
            String fileName = getFileName(name);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            convertor.parse(fileName, baos, XSLT_FILE, Thread.currentThread().getContextClassLoader());

         }
      } finally {
         Thread.currentThread().setContextClassLoader(existingCl);
      }
   }

   private String getFileName(String s) {
      return BASE_DIR + File.separator + s;
   }

   public static class TestClassLoader extends ClassLoader {
      private ClassLoader existing;

      TestClassLoader(ClassLoader existing) {
         super(existing);
         this.existing = existing;
      }

      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
         if (name.equals("org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore")) {
            return DummyInMemoryCacheStore.class;
         }
         return existing.loadClass(name);
      }
   }
}
