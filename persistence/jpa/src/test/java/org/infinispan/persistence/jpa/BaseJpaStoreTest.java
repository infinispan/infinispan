package org.infinispan.persistence.jpa;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.testng.annotations.Test;

import io.reactivex.functions.Consumer;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "persistence.BaseJpaStoreTest")
public abstract class BaseJpaStoreTest extends AbstractJpaStoreTest {
   protected abstract TestObject createTestObject(String key);

   @Test(expectedExceptions = PersistenceException.class)
   public void testStoreNoJpa() {
      assertFalse(cs.contains("k"));
      cs.write(createEntry("k", "v"));
   }

   @Test(expectedExceptions = PersistenceException.class)
   public void testStoreWithJpaBadKey() {
      assertFalse(cs.contains("k"));
      TestObject obj = createTestObject("1");
      cs.write(createEntry("k", obj.getValue()));
   }

   public void testStoreWithJpaGoodKey() {
      TestObject obj = createTestObject("testStoreWithJpaGoodKey");
      assertFalse(cs.contains(obj.getKey()));
      MarshalledEntry me = createEntry(obj);
      cs.write(me);
   }

   public void testLoadAndStoreImmortal() {
      TestObject obj = createTestObject("testLoadAndStoreImmortal");
      assertFalse(cs.contains(obj.getKey()));
      MarshalledEntry me = createEntry(obj);
      cs.write(me);

      assertTrue(cs.contains(obj.getKey()));
      assertEquals(obj.getValue(), cs.load(obj.getKey()).getValue());
      assertNull(cs.load(obj.getKey()).getMetadata());

      // TODO test with metadata

      boolean removed = cs.delete("nonExistentKey");
      assertFalse(removed);
   }

   public void testPreload() throws Exception {
      TestObject obj1 = createTestObject("testPreload1");
      TestObject obj2 = createTestObject("testPreload2");
      TestObject obj3 = createTestObject("testPreload3");

      cs.write(createEntry(obj1));
      cs.write(createEntry(obj2));
      cs.write(createEntry(obj3));
      assertEquals(cs.load(obj1.getKey()).getValue(), obj1.getValue());
      assertEquals(cs.load(obj2.getKey()).getValue(), obj2.getValue());
      assertEquals(cs.load(obj3.getKey()).getValue(), obj3.getValue());

      final ConcurrentHashMap map = new ConcurrentHashMap();
      Consumer<MarshalledEntry<Object, Object>> taskWithValues = me -> {
         if (me.getKey() != null && me.getValue() != null) {
            map.put(me.getKey(), me.getValue());
         }
      };
      cs.publishEntries(null, true, false).blockingSubscribe(taskWithValues);

      assertEquals(map.size(), 3);
      assertEquals(map.remove(obj1.getKey()), obj1.getValue());
      assertEquals(map.remove(obj2.getKey()), obj2.getValue());
      assertEquals(map.remove(obj3.getKey()), obj3.getValue());
      assertTrue(map.isEmpty());

      final ConcurrentHashSet set = new ConcurrentHashSet();
      Consumer<MarshalledEntry<Object, Object>> taskWithoutValues = me -> {
         if (me.getKey() != null) {
            set.add(me.getKey());
         }
      };

      cs.publishEntries(null, false, false).blockingSubscribe(taskWithoutValues);
      assertEquals(set.size(), 3);
      assertTrue(set.remove(obj1.getKey()));
      assertTrue(set.remove(obj2.getKey()));
      assertTrue(set.remove(obj3.getKey()));
      assertTrue(set.isEmpty());

      Set collectSet = cs.publishKeys(null).collectInto(new HashSet<>(), Set::add).blockingGet();
      assertEquals(collectSet.size(), 3);
      assertTrue(collectSet.remove(obj1.getKey()));
      assertTrue(collectSet.remove(obj2.getKey()));
      assertTrue(collectSet.remove(obj3.getKey()));
      assertTrue(collectSet.isEmpty());
   }

   public void testStoreAndRemoveAll() {
      TestObject obj1 = createTestObject("testStoreAndRemoveAll1");
      TestObject obj2 = createTestObject("testStoreAndRemoveAll2");
      TestObject obj3 = createTestObject("testStoreAndRemoveAll3");
      TestObject obj4 = createTestObject("testStoreAndRemoveAll4");

      cs.write(createEntry(obj1));
      cs.write(createEntry(obj2));
      cs.write(createEntry(obj3));
      cs.write(createEntry(obj4));

      assertEquals(cs.size(), 4);

      cs.clear();
      assertEquals(cs.size(), 0);
      assertFalse(cs.contains(obj1.getKey()));
      assertFalse(cs.contains(obj2.getKey()));
      assertFalse(cs.contains(obj3.getKey()));
      assertFalse(cs.contains(obj4.getKey()));
   }

   public void testStoreValuesViaNonJpaCacheStore() {
      TestObject obj1 = createTestObject("testStoreViaNonJpaCacheStore1");
      TestObject obj2 = createTestObject("testStoreViaNonJpaCacheStore2");

      assertEquals(cs.size(), 0);
      assertFalse(cs.contains(obj1.getKey()));
      assertFalse(cs.contains(obj1.getKey()));

      EntityManagerFactory emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME);
      EntityManager em = emf.createEntityManager();
      EntityTransaction txn = em.getTransaction();
      txn.begin();
      em.persist(obj1.getValue());
      em.persist(obj2.getValue());
      em.flush();
      txn.commit();
      em.close();

      assertEquals(cs.size(), 2);
      assertTrue(cs.contains(obj1.getKey()));
      assertTrue(cs.contains(obj1.getKey()));
   }

   public void testLoadValuesViaNonJpaCacheStore() {
      TestObject obj1 = createTestObject("testLoadViaNonJpaCacheStore1");
      TestObject obj2 = createTestObject("testLoadViaNonJpaCacheStore2");
      cs.write(createEntry(obj1));
      cs.write(createEntry(obj2));

      assertEquals(cs.size(), 2);
      assertTrue(cs.contains(obj1.getKey()));
      assertTrue(cs.contains(obj1.getKey()));

      EntityManagerFactory emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME);
      EntityManager em = emf.createEntityManager();

      assertEquals(em.find(obj1.getValue().getClass(), obj1.getKey()), obj1.getValue());
      assertEquals(em.find(obj2.getValue().getClass(), obj2.getKey()), obj2.getValue());

      em.close();
   }
}
