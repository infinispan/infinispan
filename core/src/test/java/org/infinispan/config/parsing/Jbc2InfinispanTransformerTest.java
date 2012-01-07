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

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.concurrent.ConcurrentMap;

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

         //System.out.println("Output file is:\n" + baos.toString());

         EmbeddedCacheManager ecm = new DefaultCacheManager(new ByteArrayInputStream(baos.toByteArray()), false);
         Configuration defaultConfig = ecm.getDefaultConfiguration();
         GlobalConfiguration globalConfig = ecm.getGlobalConfiguration();
         assert defaultConfig.getIsolationLevel().equals(IsolationLevel.READ_COMMITTED);
         assert defaultConfig.getLockAcquisitionTimeout() == 234000;
         assert defaultConfig.getConcurrencyLevel() == 510;
         assert defaultConfig.getTransactionManagerLookup().getClass().getName().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
         assert !defaultConfig.isSyncCommitPhase();
         assert defaultConfig.isSyncRollbackPhase();
         assert defaultConfig.isExposeJmxStatistics();
         assert globalConfig.getShutdownHookBehavior().equals(GlobalConfiguration.ShutdownHookBehavior.DONT_REGISTER);
         assert globalConfig.getAsyncListenerExecutorProperties().get("maxThreads").equals("123");
         assert globalConfig.getAsyncListenerExecutorProperties().get("queueSize").equals("1020000");
         assert !defaultConfig.isInvocationBatchingEnabled();
         assert globalConfig.getMarshallerClass().equals(VersionAwareMarshaller.class.getName());
         assert defaultConfig.isStoreAsBinary();
         assert globalConfig.getClusterName().equals("JBossCache-cluster");
         assert defaultConfig.getCacheMode().equals(Configuration.CacheMode.INVALIDATION_SYNC);
         assert defaultConfig.getStateRetrievalTimeout() == 2120000;
         assert defaultConfig.getSyncReplTimeout() == 22220000;
         assert defaultConfig.getEvictionStrategy().equals(EvictionStrategy.LRU);
         assert defaultConfig.getEvictionMaxEntries() == 5001;
         assert defaultConfig.getExpirationMaxIdle() == 1001 : "Received " + defaultConfig.getExpirationLifespan();
         assert defaultConfig.getExpirationWakeUpInterval() == 50015;

         ConcurrentMap<String, Configuration> configurationOverrides = (ConcurrentMap<String, Configuration>) TestingUtil.extractField(ecm, "configurationOverrides");

         Configuration regionOne = configurationOverrides.get("/org/jboss/data1");
         assert regionOne != null;
         assert regionOne.getEvictionStrategy().equals(EvictionStrategy.LRU);
         assert regionOne.getExpirationMaxIdle() == 2002;
         assert regionOne.getExpirationWakeUpInterval() == 50015;

         Configuration regionTwo = configurationOverrides.get("/org/jboss/data2");
         assert regionTwo != null;
         assert regionTwo.getEvictionStrategy().equals(EvictionStrategy.FIFO);
         assert regionTwo.getEvictionMaxEntries() == 3003;
         assert regionTwo.getExpirationWakeUpInterval() == 50015;


         CacheLoaderManagerConfig loaderManagerConfig = defaultConfig.getCacheLoaderManagerConfig();
         assert loaderManagerConfig.isPassivation();
         assert loaderManagerConfig.isShared();

         assert loaderManagerConfig.getCacheLoaderConfigs().size() == 1;
         CacheStoreConfig config = (CacheStoreConfig) loaderManagerConfig.getCacheLoaderConfigs().get(0);
         assert config.getCacheLoaderClassName().equals("org.infinispan.loaders.file.FileCacheStore");
         AsyncStoreConfig asyncStoreConfig = config.getAsyncStoreConfig();
         assert asyncStoreConfig != null;
         assert asyncStoreConfig.isEnabled();
         assert config.isFetchPersistentState();
         assert config.isIgnoreModifications();
         assert config.isPurgeOnStartup();
         SingletonStoreConfig singletonStoreConfig = config.getSingletonStoreConfig();
         assert singletonStoreConfig != null;
         assert singletonStoreConfig.isSingletonStoreEnabled();
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
