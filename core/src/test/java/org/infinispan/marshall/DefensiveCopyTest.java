package org.infinispan.marshall;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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
      builder.memory().storage(StorageType.BINARY);
      return TestCacheManagerFactory.createCacheManager(TestDataSCI.INSTANCE, builder);
   }

   public void testOriginalReferenceSafety() {
      Person key = new Person("Key1");
      Person value = new Person("Mr Infinispan");
      cache().put(key, value);
      assertEquals(value, cache.get(key));
      // Change referenced object
      value.setName("Ms Hibernate");
      // If defensive copies are working as expected,
      // it should be same as before
      assertEquals(new Person("Mr Infinispan"), cache.get(key));
      key.setName("Key2");
      assertNull(cache.get(key));
   }

   public void testSafetyAfterRetrieving() {
      final Integer k = 2;
      Person person = new Person("Mr Coe");
      cache().put(k, person);
      Person cachedPerson = this.<Integer, Person>cache().get(k);
      assertEquals(person, cachedPerson);
      cachedPerson.setName("Mr Digweed");
      assertEquals(person, cache.get(k));
   }
}
