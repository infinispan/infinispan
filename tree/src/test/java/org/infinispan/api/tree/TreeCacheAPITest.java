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
package org.infinispan.api.tree;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.Node;
import org.infinispan.tree.NodeKey;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheFactory;
import org.infinispan.tree.TreeCacheImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.*;

/**
 * Tests the {@link TreeCache} public API at a high level
 *
 * @author <a href="mailto:manik AT jboss DOT org">Manik Surtani</a>
 */

@Test(groups = "functional", testName = "api.tree.TreeCacheAPITest")
public class TreeCacheAPITest extends SingleCacheManagerTest {
   private TreeCache<String, String> cache;
   private TransactionManager tm;
   private Log log = LogFactory.getLog(TreeCacheAPITest.class);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      Configuration c = new Configuration();
      c.setInvocationBatchingEnabled(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c);

      Cache flatcache = cm.getCache();
      cache = new TreeCacheImpl(flatcache);

      tm = TestingUtil.getTransactionManager(flatcache);
      return cm;
   }

   public void testGetData() {
      cache.put(Fqn.fromRelativeFqn(Fqn.fromString("STATUS"), Fqn.fromString("TRADE")),"key1","TRADE1");
      cache.put(Fqn.fromRelativeFqn(Fqn.fromString("STATUS"), Fqn.fromString("TRADE")),"key2","TRADE2");
      cache.put(Fqn.fromRelativeFqn(Fqn.fromString("STATUS"), Fqn.fromString("TRADE")),"key3","TRADE3");
      cache.put(Fqn.fromRelativeFqn(Fqn.fromString("STATUS"), Fqn.fromString("TRADE")),"key4","TRADE4");
      cache.put(Fqn.fromRelativeFqn(Fqn.fromString("STATUS"), Fqn.fromString("TRADE")),"key5","TRADE5");
      cache.put(Fqn.fromRelativeFqn(Fqn.fromString("STATUS"), Fqn.fromString("TRADE")),"key6","TRADE6");
      cache.put(Fqn.fromRelativeFqn(Fqn.fromString("STATUS"), Fqn.fromString("TRADE")),"key7","TRADE7");
      Object object = cache.get(Fqn.fromRelativeFqn(Fqn.fromString("STATUS"), Fqn.fromString("TRADE")),"key7");
      assertNotNull(object);
      Map<String, String> data = cache.getData(Fqn.fromRelativeFqn(Fqn.fromString("STATUS"), Fqn.fromString("TRADE")));
      assertNotNull(data);
   }

   public void testConvenienceMethods() {
      Fqn fqn = Fqn.fromString("/test/fqn");
      String key = "key", value = "value";
      Map<String, String> data = new HashMap<String, String>();
      data.put(key, value);

      assertNull(cache.get(fqn, key));

      cache.put(fqn, key, value);

      assertEquals(value, cache.get(fqn, key));

      cache.remove(fqn, key);

      assertNull(cache.get(fqn, key));

      cache.put(fqn, data);

      assertEquals(value, cache.get(fqn, key));
   }


   /**
    * Another convenience method that tests node removal
    */
   public void testNodeConvenienceNodeRemoval() {
      // this fqn is relative, but since it is from the root it may as well be absolute
      Fqn fqn = Fqn.fromString("/test/fqn");
      cache.getRoot().addChild(fqn);
      assertTrue(cache.getRoot().hasChild(fqn));

      assertEquals(true, cache.removeNode(fqn));
      assertFalse(cache.getRoot().hasChild(fqn));
      // remove should REALLY remove though and not just mark as deleted/invalid.
      Node n = cache.getNode(fqn);
      assert n == null;

      assertEquals(false, cache.removeNode(fqn));

      // remove should REALLY remove though and not just mark as deleted/invalid.
      n = cache.getNode(fqn);
      assert n == null;

      // Check that it's removed if it has a child
      Fqn child = Fqn.fromString("/test/fqn/child");
      log.error("TEST: Adding child " + child);
      cache.getRoot().addChild(child);
      assertStructure(cache, "/test/fqn/child");

      assertEquals(true, cache.removeNode(fqn));
      assertFalse(cache.getRoot().hasChild(fqn));
      assertEquals(false, cache.removeNode(fqn));
   }

   private void assertStructure(TreeCache tc, String fqnStr) {
      // make sure structure nodes are properly built and maintained
      Cache c = tc.getCache();
      Fqn fqn = Fqn.fromString(fqnStr);
      // loop thru the Fqn, starting at its root, and make sure all of its children exist in proper NodeKeys
      for (int i = 0; i < fqn.size(); i++) {
         Fqn parent = fqn.getSubFqn(0, i);
         Object childName = fqn.get(i);
         // make sure a data key exists in the cache
         assert c.containsKey(new NodeKey(parent, NodeKey.Type.DATA)) : "Node [" + parent + "] does not have a Data atomic map!";
         assert c.containsKey(new NodeKey(parent, NodeKey.Type.STRUCTURE)) : "Node [" + parent + "] does not have a Structure atomic map!";
         AtomicMap<Object, Fqn> am = AtomicMapLookup.getAtomicMap(c, new NodeKey(parent, NodeKey.Type.STRUCTURE));
         boolean hasChild = am.containsKey(childName);
         assert hasChild : "Node [" + parent + "] does not have a child [" + childName + "] in its Structure atomic map!";
      }
   }


   public void testStopClearsData() throws Exception {
      Fqn a = Fqn.fromString("/a");
      Fqn b = Fqn.fromString("/a/b");
      String key = "key", value = "value";
      cache.getRoot().addChild(a).put(key, value);
      cache.getRoot().addChild(b).put(key, value);
      cache.getRoot().put(key, value);

      assertEquals(value, cache.getRoot().get(key));
      assertEquals(value, cache.getRoot().getChild(a).get(key));
      assertEquals(value, cache.getRoot().getChild(b).get(key));

      cache.stop();

      cache.start();

      assertNull(cache.getRoot().get(key));
      assertTrue(cache.getRoot().getData().isEmpty());
      assertTrue(cache.getRoot().getChildren().isEmpty());
   }

   public void testPhantomStructuralNodesOnRemove() {
      assert cache.getNode(Fqn.fromString("/a/b/c")) == null;
      assert !cache.removeNode("/a/b/c");
      assert cache.getNode(Fqn.fromString("/a/b/c")) == null;
      assert cache.getNode(Fqn.fromString("/a/b")) == null;
      assert cache.getNode(Fqn.fromString("/a")) == null;
   }

   public void testPhantomStructuralNodesOnRemoveTransactional() throws Exception {
      assert cache.getNode(Fqn.fromString("/a/b/c")) == null;
      tm.begin();
      assert !cache.removeNode("/a/b/c");
      tm.commit();
      assert cache.getNode(Fqn.fromString("/a/b/c")) == null;
      assert cache.getNode(Fqn.fromString("/a/b")) == null;
      assert cache.getNode(Fqn.fromString("/a")) == null;
   }

   public void testRpcManagerElements() {
      assertEquals("CacheMode.LOCAL cache has no address", null, manager(cache.getCache()).getAddress());
      assertEquals("CacheMode.LOCAL cache has no members list", null, manager(cache.getCache()).getMembers());
   }

   public void testTreeCacheFactory() throws Exception {
      withCacheManager(new Callable<EmbeddedCacheManager>() {
         @Override
         public EmbeddedCacheManager call() throws Exception {
            org.infinispan.configuration.cache.Configuration config = new ConfigurationBuilder().invocationBatching().enable().build();
            DefaultCacheManager cm = new DefaultCacheManager(config);
            TreeCacheFactory tcf = new TreeCacheFactory();
            tcf.createTreeCache(cm.getCache());
            return cm;
         }

      });

   }

   @Test(expectedExceptions=ConfigurationException.class)
   public void testFactoryNoBatching() throws Exception {
      withCacheManager(new Callable<EmbeddedCacheManager>() {
         @Override
         public EmbeddedCacheManager call() throws Exception {
            org.infinispan.configuration.cache.Configuration config = new ConfigurationBuilder().build();
            DefaultCacheManager cm = new DefaultCacheManager(config);
            TreeCacheFactory tcf = new TreeCacheFactory();
            tcf.createTreeCache(cm.getCache());
            return cm;
         }
      });
   }
}
