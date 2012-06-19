/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.Interpreter;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName="cli-server.InterpretTest")
public class InterpreterTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.jmxStatistics().enable().dataContainer().invocationBatching().enable();
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testSimple() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = interpreter.createSessionId();
      interpreter.execute(sessionId, "put 'a' 'b'; get 'a';");
      interpreter
            .execute(sessionId, "put 'c' {\"org.infinispan.cli.interpreter.MyClass\":{\"i\":5,\"x\":null,\"b\":true}};");
      Object o = cache.get("c");
      assert o != null;
      assert o instanceof MyClass;
      assert ((MyClass) o).i == 5;
      assert ((MyClass) o).b;
      interpreter.execute(sessionId, "put 'f' 0.5;");
      Double f = (Double) cache.get("f");
      assert f == 0.5;
   }

   public void testCacheQualifier() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = interpreter.createSessionId();
      Cache<Object, Object> otherCache = cacheManager.getCache("otherCache");
      interpreter.execute(sessionId, "put 'a' 'a'; put 'otherCache'.'b' 'b'; cache 'otherCache'; put 'c' 'c';");
      Object a = cache.get("a");
      assert a.equals("a");
      Object b = otherCache.get("b");
      assert b.equals("b");
      Object c = otherCache.get("c");
      assert c.equals("c");
   }

   public void testBatching() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = interpreter.createSessionId();
      interpreter.execute(sessionId, "start; put 'a' 'a'; put 'b' 'b'; end;");
      Object a = cache.get("a");
      assert a.equals("a");
      Object b = cache.get("b");
      assert b.equals("b");
   }

   public void testTx() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = interpreter.createSessionId();
      interpreter.execute(sessionId, "begin; put 'a' 'a'; commit;");
      Object a = cache.get("a");
      assert a.equals("a");
      interpreter.execute(sessionId, "begin; put 'b' 'b'; rollback;");
      assert !cache.containsKey("b");
   }

   public void testDangling() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = interpreter.createSessionId();
      interpreter.execute(sessionId, "begin; put 'a' 'a';");
      assert cache.getAdvancedCache().getTransactionManager().getTransaction() == null;
      assert !cache.containsKey("a");
      interpreter.execute(sessionId, "start; put 'a' 'a';");
      assert cache.getAdvancedCache().getBatchContainer().getBatchTransaction() == null;
      assert !cache.containsKey("a");
   }

   public void testRemove() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = interpreter.createSessionId();
      interpreter.execute(sessionId, "put 'a' 'a';");
      Object a = cache.get("a");
      assert a.equals("a");
      interpreter.execute(sessionId, "remove 'a';");
      assert !cache.containsKey("a");
      interpreter.execute(sessionId, "put 'b' 'b';");
      Object b = cache.get("b");
      assert b.equals("b");
      interpreter.execute(sessionId, "remove 'b' 'c';");
      assert cache.containsKey("b");
   }

   public void testReplace() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = interpreter.createSessionId();
      interpreter.execute(sessionId, "put 'a' 'a';");
      Object a = cache.get("a");
      assert a.equals("a");
      interpreter.execute(sessionId, "replace 'a' 'b';");
      a = cache.get("a");
      assert a.equals("b");
      interpreter.execute(sessionId, "replace 'a' 'b' 'c';");
      a = cache.get("a");
      assert a.equals("c");
      interpreter.execute(sessionId, "replace 'a' 'b' 'd';");
      a = cache.get("a");
      assert a.equals("c");
   }

   @Test(expectedExceptions=IllegalArgumentException.class)
   public void testInvalidSession() throws Exception {
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(this.cacheManager);
      Interpreter interpreter = gcr.getComponent(Interpreter.class);
      String sessionId = "123";
      interpreter.execute(sessionId, "put 'a' 'a';");
   }
}
