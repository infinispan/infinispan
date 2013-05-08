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
package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerXmlConfigurationTest")
public class CacheManagerXmlConfigurationTest extends AbstractInfinispanTest {
   EmbeddedCacheManager cm;

   @AfterMethod
   public void tearDown() {
      if (cm != null) cm.stop();
      cm =null;
   }

   public void testNamedCacheXML() throws IOException {
      cm = TestCacheManagerFactory.fromXml("configs/named-cache-test.xml");

      assertEquals("s1", cm.getGlobalConfiguration().getSiteId());
      assertEquals("r1", cm.getGlobalConfiguration().getRackId());
      assertEquals("m1", cm.getGlobalConfiguration().getMachineId());

      // test default cache
      Cache c = cm.getCache();
      assert c.getConfiguration().getConcurrencyLevel() == 100;
      assert c.getConfiguration().getLockAcquisitionTimeout() == 1000;
      assert !c.getConfiguration().isTransactionalCache();
      assertEquals(c.getConfiguration().getTransactionMode(), TransactionMode.NON_TRANSACTIONAL);
      assert TestingUtil.extractComponent(c, Transport.class) != null : "This should not be null, since a shared transport should be present";

      // test the "transactional" cache
      c = cm.getCache("transactional");
      assert c.getConfiguration().isTransactionalCache();
      assert c.getConfiguration().getConcurrencyLevel() == 100;
      assert c.getConfiguration().getLockAcquisitionTimeout() == 1000;
      assert TestingUtil.extractComponent(c, TransactionManager.class) != null;
      assert TestingUtil.extractComponent(c, Transport.class) != null : "This should not be null, since a shared transport should be present";

      // test the "replicated" cache
      c = cm.getCache("syncRepl");
      assert c.getConfiguration().getConcurrencyLevel() == 100;
      assert c.getConfiguration().getLockAcquisitionTimeout() == 1000;
      assertEquals(c.getConfiguration().getTransactionMode(), TransactionMode.NON_TRANSACTIONAL);
      assert TestingUtil.extractComponent(c, Transport.class) != null : "This should not be null, since a shared transport should be present";

      // test the "txSyncRepl" cache
      c = cm.getCache("txSyncRepl");
      assert c.getConfiguration().getConcurrencyLevel() == 100;
      assert c.getConfiguration().getLockAcquisitionTimeout() == 1000;
      assert TestingUtil.extractComponent(c, TransactionManager.class) != null;
      assert TestingUtil.extractComponent(c, Transport.class) != null : "This should not be null, since a shared transport should be present";
   }

   public void testNamedCacheXMLClashingNames() {
      String xml = INFINISPAN_START_TAG +
            "\n" +
            "    <default>\n" +
            "        <locking concurrencyLevel=\"100\" lockAcquisitionTimeout=\"1000\" />\n" +
            "    </default>\n" +
            "\n" +
            "    <namedCache name=\"c1\">\n" +
            "        <transaction transactionManagerLookupClass=\"org.infinispan.transaction.GenericTransactionManagerLookup\"/>\n" +
            "    </namedCache>\n" +
            "\n" +
            "    <namedCache name=\"c1\">\n" +
            "        <clustering>\n" +
            "            <sync replTimeout=\"15000\"/>\n" +
            "        </clustering>\n" +
            "    </namedCache>\n" +
            INFINISPAN_END_TAG;

      ByteArrayInputStream bais = new ByteArrayInputStream(xml.getBytes());
      try {
         cm = TestCacheManagerFactory.fromStream(bais);
         assert false : "Should fail";
      } catch (Throwable expected) {
      }
   }

   public void testNamedCacheXMLClashingNamesProgrammatic() throws IOException {
      String xml = INFINISPAN_START_TAG +
            "\n" +
            "<global/>\n" +
            "    <default>\n" +
            "        <locking concurrencyLevel=\"100\" lockAcquisitionTimeout=\"1000\" />\n" +
            "    </default>\n" +
            "\n" +
            "    <namedCache name=\"c1\">\n" +
            "        <transaction transactionManagerLookupClass=\"org.infinispan.transaction.lookup.GenericTransactionManagerLookup\"/>\n" +
            "    </namedCache>\n" + INFINISPAN_END_TAG;

      ByteArrayInputStream bais = new ByteArrayInputStream(xml.getBytes());
      cm = TestCacheManagerFactory.fromStream(bais);

      assert cm.getCache() != null;
      assert cm.getCache("c1") != null;
      Configuration c1Config = cm.getCache("c1").getConfiguration();
      assert c1Config != null;
      Configuration redefinedConfig = cm.defineConfiguration("c1", new Configuration());
      assert c1Config.equals(redefinedConfig);
   }

   public void testDeprecatedElements() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromXml("configs/deprecated-elements.xml");
      try {
         Cache[] caches = new Cache[]{cm.getCache("storeAsBinary"), cm.getCache()};
         for (Cache c : caches) {
            assert c.getCacheConfiguration().storeAsBinary().enabled();
            assert c.getCacheConfiguration().expiration().wakeUpInterval() == 12000;
         }
      } finally {
         cm.stop();
      }
   }

   public void testBatchingIsEnabled() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromXml("configs/batching.xml");
      try {
         Cache c = cm.getCache("any");
         assert c.getConfiguration().isInvocationBatchingEnabled();
         assert c.getConfiguration().isTransactionalCache();
         c = cm.getCache();
         assert c.getConfiguration().isInvocationBatchingEnabled();
         Cache c2 = cm.getCache("tml");
         assert c2.getConfiguration().isTransactionalCache();
      } finally {
         cm.stop();
      }
   }

   public void testCreateWithMultipleXmlFiles() throws Exception {
      String xmlFile = "configs/local-singlenamedcache-test.xml";
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(xmlFile, xmlFile, xmlFile)) {
         @Override
         public void call() {
            Cache<Object, Object> c = cm.getCache();
            assert c.getCacheConfiguration().locking().lockAcquisitionTimeout() == 1111;
            Cache<Object, Object> c2 = cm.getCache("localCache");
            assert c2.getCacheConfiguration().locking().lockAcquisitionTimeout() == 22222;
            GlobalConfiguration globalCfg = cm.getCacheManagerConfiguration();
            assert globalCfg.asyncListenerExecutor().properties()
                  .get("threadNamePrefix").equals("Any-AsyncListenerThread");
         }
      });
   }

}


