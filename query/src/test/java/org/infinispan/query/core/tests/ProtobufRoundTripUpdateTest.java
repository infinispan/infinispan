package org.infinispan.query.core.tests;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * UPDATE statements on a cache with default Java-object storage whose entity is
 * protobuf-marshallable: the {@code PROTOBUF_ROUNDTRIP} strategy. A statement
 * with only constant values round-trips the entity through protobuf, while a
 * statement with a value expression mutates the entity in place via
 * reflection; the two tests pin both behaviours for the same cache.
 */
@Test(groups = "functional", testName = "query.core.tests.ProtobufRoundTripUpdateTest")
public class ProtobufRoundTripUpdateTest extends SingleCacheManagerTest {

   private static final String ENTITY = Wallet.class.getName();

   private Cache<String, Wallet> walletCache;

   @ProtoSchema(includeClasses = Wallet.class, schemaPackageName = "org.infinispan.query.core.tests", service = false)
   interface WalletSchema extends GeneratedSchema {
      WalletSchema INSTANCE = new WalletSchemaImpl();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      cacheManager = TestCacheManagerFactory.createCacheManager(WalletSchema.INSTANCE, null);
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      walletCache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("roundtrip-update", builder.build());
      return cacheManager;
   }

   @BeforeMethod
   public void populate() {
      walletCache.clear();
      walletCache.put("w1", wallet("Tom", 100));
   }

   private static Wallet wallet(String owner, long balance) {
      Wallet wallet = new Wallet();
      wallet.setOwner(owner);
      wallet.setBalance(balance);
      return wallet;
   }

   @Test
   public void testConstantUpdateRoundTripsThroughProtobuf() {
      // constant value: the entity is serialized to protobuf, updated and
      // deserialized back
      Query<Wallet> update = walletCache.query(
            "UPDATE FROM " + ENTITY + " SET owner = 'Thomas' WHERE owner = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);

      Wallet wallet = walletCache.get("w1");
      assertThat(wallet.getOwner()).isEqualTo("Thomas");
      // the untouched field survives the round trip
      assertThat(wallet.getBalance()).isEqualTo(100);
   }

   @Test
   public void testExpressionUpdateMutatesTheEntityInPlace() {
      // expression value: the same cache applies the update in place, through
      // the reflection branch of the expression path
      Query<Wallet> update = walletCache.query(
            "UPDATE FROM " + ENTITY + " SET balance = balance + 50 WHERE owner = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(walletCache.get("w1").getBalance()).isEqualTo(150);
   }

   @Test
   public void testSetNullRemovesTheField() {
      // protobuf round-trip semantics: assigning null removes the field
      Query<Wallet> update = walletCache.query(
            "UPDATE FROM " + ENTITY + " SET owner = null WHERE owner = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(walletCache.get("w1").getOwner()).isNull();
      assertThat(walletCache.get("w1").getBalance()).isEqualTo(100);
   }
}
