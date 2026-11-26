package org.infinispan.persistence.jdbc.stringbased;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.sql.Statement;
import java.util.List;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;

/**
 * Tester class  for {@link JdbcStringBasedStore}.
 *
 * @author William Burns
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreTest")
public class JdbcStringBasedStoreTest extends BaseNonBlockingStoreTest {

   boolean segmented;

   public JdbcStringBasedStoreTest segmented(boolean segmented) {
      this.segmented = segmented;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new JdbcStringBasedStoreTest().segmented(false),
            new JdbcStringBasedStoreTest().segmented(true),
      };
   }

   @Override
   protected String parameters() {
      return "[" + segmented + "]";
   }

   @Override
   protected JdbcStringBasedStore createStore() throws Exception {
      return new JdbcStringBasedStore();
   }

   public void testConcurrentUpdateDuringPurge() throws Exception {
      store.write(marshalledEntry(internalEntryFactory.create("key", "value", null, 1000, -1)));
      timeService.advance(1001);

      JdbcStringBasedStore<?, ?> jdbcStore = (JdbcStringBasedStore<?, ?>) store.delegate();
      List<?> candidates = invokePurgePhase(jdbcStore, "getEntriesToBeDeleted", new Class<?>[0]);
      assertEquals(1, candidates.size());

      // Refresh the same value after selection, before the conditional JDBC delete.
      store.write(marshalledEntry(internalEntryFactory.create("key", "value", null, 60000, -1)));
      int[] deleted = invokePurgePhase(jdbcStore, "deleteEntries", new Class<?>[]{List.class}, candidates);
      assertArrayEquals(new int[]{0}, deleted);

      var notifications = UnicastProcessor.<MarshallableEntry<Object, Object>>create();
      long purged = invokePurgePhase(jdbcStore, "doNotify",
            new Class<?>[]{List.class, int[].class, FlowableProcessor.class}, candidates, deleted, notifications);
      notifications.onComplete();

      assertEquals("value", store.loadEntry("key").getValue());
      assertTrue(notifications.toList().blockingGet().isEmpty(),
            "A concurrent update must not produce a stale expiration notification");
      assertEquals(0, purged);
   }

   @DataProvider(name = "purgeDeleteResults")
   public Object[][] purgeDeleteResults() {
      return new Object[][]{
            {0, 0L},
            {1, 1L},
            {Statement.SUCCESS_NO_INFO, 1L},
            {Statement.EXECUTE_FAILED, 0L},
      };
   }

   @Test(dataProvider = "purgeDeleteResults")
   public void testPurgeNotificationForDeleteResult(int deleteResult, long expectedNotifications) throws Exception {
      store.write(marshalledEntry(internalEntryFactory.create("key", "value", null, 1000, -1)));
      timeService.advance(1001);
      JdbcStringBasedStore<?, ?> jdbcStore = (JdbcStringBasedStore<?, ?>) store.delegate();
      List<?> candidates = invokePurgePhase(jdbcStore, "getEntriesToBeDeleted", new Class<?>[0]);
      assertEquals(1, candidates.size());

      var notifications = UnicastProcessor.<MarshallableEntry<Object, Object>>create();
      long purged = invokePurgePhase(jdbcStore, "doNotify",
            new Class<?>[]{List.class, int[].class, FlowableProcessor.class},
            candidates, new int[]{deleteResult}, notifications);
      notifications.onComplete();
      assertEquals(expectedNotifications, notifications.toList().blockingGet().size());
      assertEquals(expectedNotifications, purged);
   }

   // Invoke the real purge phases separately to reproduce the interleaving without sleeps or mocks.
   @SuppressWarnings("unchecked")
   private static <T> T invokePurgePhase(JdbcStringBasedStore<?, ?> store, String name,
                                       Class<?>[] parameterTypes, Object... arguments) throws Exception {
      Method method = JdbcStringBasedStore.class.getDeclaredMethod(name, parameterTypes);
      method.setAccessible(true);
      return (T) method.invoke(store, arguments);
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder configurationBuilder) {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = configurationBuilder
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      storeBuilder.segmented(segmented);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      return configurationBuilder.build();
   }
}
