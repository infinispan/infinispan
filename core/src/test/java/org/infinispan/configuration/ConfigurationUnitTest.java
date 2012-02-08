/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.configuration;

import static org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;
import static org.testng.Assert.assertEquals;

import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.FileLookup;
import org.infinispan.util.FileLookupFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "config.ConfigurationUnitTest")
public class ConfigurationUnitTest {
   
   
   @Test
   public void testBuild() {
      // Simple test to ensure we can actually build a config
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.build();
   }
   
   @Test
   public void testCreateCache() {
      DefaultCacheManager cm = new DefaultCacheManager(new ConfigurationBuilder().build());
      TestingUtil.killCacheManagers(cm);
   }
   
   @Test
   public void testAdapt() {
      // Simple test to ensure we can actually adapt a config to the old config
      ConfigurationBuilder cb = new ConfigurationBuilder();
      LegacyConfigurationAdaptor.adapt(cb.build());
   }
   
   @Test
   public void testEvictionMaxEntries() {
      Configuration configuration = new ConfigurationBuilder()
         .eviction().maxEntries(20)
         .build();
      org.infinispan.config.Configuration legacy = LegacyConfigurationAdaptor.adapt(configuration);
      Assert.assertEquals(legacy.getEvictionMaxEntries(), 20);
   }
   
   @Test
   public void testDistSyncAutoCommit() {
      Configuration configuration = new ConfigurationBuilder()
         .clustering().cacheMode(CacheMode.DIST_SYNC)
         .transaction().autoCommit(true)
         .build();
      org.infinispan.config.Configuration legacy = LegacyConfigurationAdaptor.adapt(configuration);
      Assert.assertTrue(legacy.isTransactionAutoCommit());
      Assert.assertEquals(legacy.getCacheMode().name(), CacheMode.DIST_SYNC.name());
   }
   
  @Test
  public void testDummyTMGetCache() {
     ConfigurationBuilder cb = new ConfigurationBuilder();
     cb.transaction().use1PcForAutoCommitTransactions(true)
        .transactionManagerLookup(new DummyTransactionManagerLookup());
     DefaultCacheManager cm = new DefaultCacheManager(cb.build());
     try {
        cm.getCache();
     } finally {
        TestingUtil.killCacheManagers(cm);
     }
  }
   
   @Test
   public void testGetCache() {
      DefaultCacheManager cm = new DefaultCacheManager(new ConfigurationBuilder().build());
      try {
         cm.getCache();
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
   
   @Test
   public void testDefineNamedCache() {
      DefaultCacheManager cacheManager = new DefaultCacheManager(new ConfigurationBuilder().build());
      try {
         cacheManager.defineConfiguration("foo", new ConfigurationBuilder().build());
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }
   
   @Test
   public void testGetAndPut() {
   // new configuration
      DefaultCacheManager cacheManager = new DefaultCacheManager(new ConfigurationBuilder().build());

      try {
         Cache<String, String> cache = cacheManager.getCache();
         cache.put("Foo", "2");
         cache.put("Bar", "4");

         Assert.assertEquals(cache.get("Foo"), "2");
         Assert.assertEquals(cache.get("Bar"), "4");
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }
   
   @Test
   public void testReplAsyncWithQueue() {
      Configuration configuration = new ConfigurationBuilder()
         .clustering().cacheMode(CacheMode.REPL_ASYNC)
         .async().useReplQueue(true).replQueueInterval(1222)
         .build();
      org.infinispan.config.Configuration legacy = LegacyConfigurationAdaptor.adapt(configuration);
   }
   
   @Test(expectedExceptions=IllegalStateException.class)
   public void testInvocationBatchingAndNonTransactional() {
      Configuration c = new ConfigurationBuilder()
         .transaction()
            .transactionMode(NON_TRANSACTIONAL)
         .invocationBatching()
            .enable()
         .build();
      DefaultCacheManager cm = new DefaultCacheManager(c);
      TestingUtil.killCacheManagers(cm);
   }
   
   @Test
   public void testConsistentHash() {
      Configuration config = LegacyConfigurationAdaptor.adapt(new org.infinispan.config.Configuration());
      Assert.assertNull(config.clustering().hash().consistentHash());
   }

   @Test
   public void testDisableL1() {
      EmbeddedCacheManager manager = TestCacheManagerFactory
            .createClusteredCacheManager(new ConfigurationBuilder(), new TransportFlags());
      try {
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.clustering().cacheMode(CacheMode.DIST_SYNC).l1().disable().disableOnRehash();
         manager.defineConfiguration("testConfigCache", cb.build());
         Cache<Object, Object> cache = manager.getCache("testConfigCache");
         assert !cache.getCacheConfiguration().clustering().l1().enabled();
         assert !cache.getCacheConfiguration().clustering().l1().onRehash();
      } finally {
         TestingUtil.killCacheManagers(manager);
      }
   }
   
   @Test
   public void testClearCacheLoaders() {
      Configuration c = new ConfigurationBuilder()
            .loaders()
               .addCacheLoader()
            .loaders()
               .clearCacheLoaders()
         .build();
      assertEquals(c.loaders().cacheLoaders().size(), 0);
   }
   
   @Test
   public void testSchema() throws Exception {
      FileLookup lookup = FileLookupFactory.newInstance();
      URL schemaFile = lookup.lookupFileLocation("schema/infinispan-config-5.2.xsd", Thread.currentThread().getContextClassLoader());
      Source xmlFile = new StreamSource(lookup.lookupFile("configs/all.xml", Thread.currentThread().getContextClassLoader()));
      SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(schemaFile).newValidator().validate(xmlFile);
   }

}
