package org.horizon.api;

import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.config.ConfigurationException;
import org.horizon.lock.IsolationLevel;
import org.horizon.manager.CacheManager;
import org.horizon.test.SingleCacheManagerTest;
import org.horizon.test.TestingUtil;
import org.horizon.transaction.DummyTransactionManager;
import org.horizon.transaction.DummyTransactionManagerLookup;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the {@link org.horizon.Cache} public API at a high level
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 */
@Test(groups = "functional")
public abstract class CacheAPITest extends SingleCacheManagerTest {
   Cache cache;

   protected CacheManager createCacheManager() throws Exception {
      // start a single cache instance
      Configuration c = new Configuration();
      c.setIsolationLevel(getIsolationLevel());
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      CacheManager cm = TestingUtil.createLocalCacheManager();
      cm.defineCache("test", c);
      cache = cm.getCache("test");
      return cm;
   }

   protected abstract IsolationLevel getIsolationLevel();

   /**
    * Tests that the configuration contains the values expected, as well as immutability of certain elements
    */
   public void testConfiguration() {
      Configuration c = cache.getConfiguration();
      assertEquals(Configuration.CacheMode.LOCAL, c.getCacheMode());
      assertEquals(DummyTransactionManagerLookup.class.getName(), c.getTransactionManagerLookupClass());

      // note that certain values should be immutable.  E.g., CacheMode cannot be changed on the fly.
      try {
         c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
         assert false : "Should have thrown an Exception";
      }
      catch (ConfigurationException e) {
         // expected
      }

      // others should be changeable though.
      c.setLockAcquisitionTimeout(100);
   }

   public void testGetMembersInLocalMode() {
      assert cache.getCacheManager().getAddress() == null : "Cache members should be null if running in LOCAL mode";
   }

   public void testConvenienceMethods() {
      String key = "key", value = "value";
      Map<String, String> data = new HashMap<String, String>();
      data.put(key, value);

      assertNull(cache.get(key));

      cache.put(key, value);

      assertEquals(value, cache.get(key));

      cache.remove(key);

      assertNull(cache.get(key));

      cache.putAll(data);

      assertEquals(value, cache.get(key));
   }

   /**
    * Tests basic eviction
    */
   public void testEvict() {
      String key1 = "keyOne", key2 = "keyTwo", value = "value";

      cache.put(key1, value);
      cache.put(key2, value);

      assert cache.containsKey(key1);
      assert cache.containsKey(key2);
      assert cache.size() == 2;

      // evict two
      cache.evict(key2);

      assert cache.containsKey(key1);
      assert !cache.containsKey(key2);
      assert cache.size() == 1;

      cache.evict(key1);

      assert !cache.containsKey(key1);
      assert !cache.containsKey(key2);
      assert cache.size() == 0;
   }

   public void testStopClearsData() throws Exception {
      String key = "key", value = "value";
      cache.put(key, value);
      assert cache.get(key).equals(value);
      assert 1 == cache.size();
      cache.stop();

      cache.start();

      assert !cache.containsKey(key);
      assert cache.isEmpty();
   }

   public void testRollbackAfterPut() throws Exception {
      String key = "key", value = "value";
      cache.put(key, value);
      assert cache.get(key).equals(value);
      assert 1 == cache.size();

      DummyTransactionManager.getInstance().begin();
      cache.put("key2", "value2");
      assert cache.get("key2").equals("value2");
      DummyTransactionManager.getInstance().rollback();

      assert cache.get(key).equals(value);
      assert 1 == cache.size();
   }

   public void testRollbackAfterOverwrite() throws Exception {
      String key = "key", value = "value";
      cache.put(key, value);
      assert cache.get(key).equals(value);
      assert 1 == cache.size();

      DummyTransactionManager.getInstance().begin();
      cache.put(key, "value2");
      assert cache.get(key).equals("value2");
      assert 1 == cache.size();
      DummyTransactionManager.getInstance().rollback();

      assert cache.get(key).equals(value);
      assert 1 == cache.size();
   }

   public void testRollbackAfterRemove() throws Exception {
      String key = "key", value = "value";
      cache.put(key, value);
      assert cache.get(key).equals(value);
      assert 1 == cache.size();

      DummyTransactionManager.getInstance().begin();
      cache.remove(key);
      assert cache.get(key) == null;
      DummyTransactionManager.getInstance().rollback();

      assert cache.get(key).equals(value);
      assert 1 == cache.size();
   }

   public void testRollbackAfterClear() throws Exception {
      String key = "key", value = "value";
      cache.put(key, value);
      assert cache.get(key).equals(value);
      assert 1 == cache.size();

      DummyTransactionManager.getInstance().begin();
      cache.clear();
      assert cache.get(key) == null;
      DummyTransactionManager.getInstance().rollback();

      assert cache.get(key).equals(value);
      assert 1 == cache.size();
   }

   public void testConcurrentMapMethods() {

      assert ((Cache<String, String>) cache).putIfAbsent("A", "B") == null;
      assert ((Cache<String, String>) cache).putIfAbsent("A", "C").equals("B");
      assert ((Cache<String, String>) cache).get("A").equals("B");

      assert !((Cache<String, String>) cache).remove("A", "C");
      assert ((Cache<String, String>) cache).containsKey("A");
      assert ((Cache<String, String>) cache).remove("A", "B");
      assert !((Cache<String, String>) cache).containsKey("A");

      ((Cache<String, String>) cache).put("A", "B");

      assert !((Cache<String, String>) cache).replace("A", "D", "C");
      assert ((Cache<String, String>) cache).get("A").equals("B");
      assert ((Cache<String, String>) cache).replace("A", "B", "C");
      assert ((Cache<String, String>) cache).get("A").equals("C");

      assert ((Cache<String, String>) cache).replace("A", "X").equals("C");
      assert ((Cache<String, String>) cache).replace("X", "A") == null;
      assert !((Cache<String, String>) cache).containsKey("X");
   }

   public void testSizeAndContents() throws Exception {
      String key = "key", value = "value";

      assert cache.isEmpty();
      assert cache.size() == 0;
      assert !cache.containsKey(key);

      cache.put(key, value);
      assert cache.size() == 1;
      assert cache.containsKey(key);
      assert !cache.isEmpty();

      assert cache.remove(key).equals(value);

      assert cache.isEmpty();
      assert cache.size() == 0;
      assert !cache.containsKey(key);

      Map<String, String> m = new HashMap<String, String>();
      m.put("1", "one");
      m.put("2", "two");
      m.put("3", "three");
      cache.putAll(m);

      assert cache.get("1").equals("one");
      assert cache.get("2").equals("two");
      assert cache.get("3").equals("three");
      assert cache.size() == 3;

      m = new HashMap<String, String>();
      m.put("1", "newvalue");
      m.put("4", "four");

      cache.putAll(m);

      assert cache.get("1").equals("newvalue");
      assert cache.get("2").equals("two");
      assert cache.get("3").equals("three");
      assert cache.get("4").equals("four");
      assert cache.size() == 4;
   }
}
