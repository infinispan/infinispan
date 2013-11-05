package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.data.Key;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.testng.annotations.Test;

import java.net.URL;
import java.net.URLClassLoader;

import static org.testng.AssertJUnit.*;

/**
 * Tests defensive copy logic.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "marshall.DefensiveCopyTest")
public class DefensiveCopyTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.storeAsBinary().enable();
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testOriginalReferenceSafety() {
      final Integer k = 1;
      Person person = new Person("Mr Infinispan");
      cache().put(k, person);
      assertEquals(person, cache.get(k));
      // Change referenced object
      person.setName("Ms Hibernate");
      // If defensive copies are working as expected,
      // it should be same as before
      assertEquals(new Person("Mr Infinispan"), cache.get(k));
   }

   public void testSafetyAfterRetrieving() {
      final Integer k = 2;
      Person person = new Person("Mr Coe");
      cache().put(k, person);
      Person cachedPerson = this.<Integer, Person>cache().get(k);
      assertEquals(person, cachedPerson);
      cachedPerson.setName("Mr Digweed");
      assertEquals(new Person("Mr Coe"), cache.get(k));
   }

   public void testDiffClassloaders() throws Exception {
      URL core = ReclosableLatch.class.getProtectionDomain().getCodeSource().getLocation();
      URL tests = getClass().getResource("/");
      ClassLoader cl1 = new URLClassLoader(new URL[]{core, tests}, null);
      ClassLoader cl2 = new URLClassLoader(new URL[]{core, tests}, null);
      Object key1 = cl1.loadClass(Key.class.getName()).getConstructor(String.class, Boolean.TYPE).newInstance("key1", false);
      Object key2 = cl2.loadClass(Key.class.getName()).getConstructor(String.class, Boolean.TYPE).newInstance("key1", false);
      String value = "tralala";
      cache.put(key1, value);
      Object result = cache.get(key2);
      assertNotNull(result);
      assertEquals(value, result);
   }

}
