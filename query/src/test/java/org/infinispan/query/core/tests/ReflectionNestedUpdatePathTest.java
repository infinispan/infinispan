package org.infinispan.query.core.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * UPDATE statements with a nested property path on a reflection-backed entity
 * (default Java-object storage, no protobuf schema): the apply path that
 * traverses getters to the nested target, mutates it in place, and rolls the
 * mutations back when a later operation of the same statement fails.
 * <p>
 * The statements use value expressions, so they run through the expression
 * apply path ({@code applyExpressionUpdate}); at least one value per statement
 * must be an expression, as a statement of only plain constants takes the
 * legacy non-expression path.
 */
@Test(groups = "functional", testName = "query.core.tests.ReflectionNestedUpdatePathTest")
public class ReflectionNestedUpdatePathTest extends SingleCacheManagerTest {

   private static final String PROFILE_ENTITY = Profile.class.getName();

   private Cache<Integer, Profile> profileCache;

   @Indexed
   public static class Profile {
      public String name;
      public Address address;
   }

   public static class Address {
      public String street;
      public int number;
      public List<String> tags;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(PROFILE_ENTITY);

      cacheManager = TestCacheManagerFactory.createCacheManager((ConfigurationBuilder) null);
      profileCache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("reflection-nested-update", builder.build());
      return cacheManager;
   }

   @BeforeMethod
   public void populate() {
      profileCache.clear();
      profileCache.put(1, makeProfile());
   }

   private static Profile makeProfile() {
      Profile profile = new Profile();
      profile.name = "Tom";
      Address address = new Address();
      address.street = "Old Street";
      address.number = 7;
      address.tags = new ArrayList<>(List.of("old", "tag"));
      profile.address = address;
      return profile;
   }

   private static Profile makeProfileWithoutAddress() {
      Profile profile = new Profile();
      profile.name = "Adrian";
      return profile;
   }

   // --- SET with a value expression on a nested path ---

   @Test
   public void testSetNestedStringFieldWithExpression() {
      Query<Profile> update = profileCache.query(
            "UPDATE FROM " + PROFILE_ENTITY + " SET address.street = 'New' + ' Street' WHERE name = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(profileCache.get(1).address.street).isEqualTo("New Street");
   }

   @Test
   public void testSetNestedNumericFieldWithExpression() {
      Query<Profile> update = profileCache.query(
            "UPDATE FROM " + PROFILE_ENTITY + " SET address.number = address.number + 1 WHERE name = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(profileCache.get(1).address.number).isEqualTo(8);
   }

   @Test
   public void testSetNestedFieldFromTopLevelProperty() {
      Query<Profile> update = profileCache.query(
            "UPDATE FROM " + PROFILE_ENTITY + " SET address.street = name WHERE name = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(profileCache.get(1).address.street).isEqualTo("Tom");
   }

   @Test
   public void testMultipleOperationsOnTheSameNestedObject() {
      Query<Profile> update = profileCache.query(
            "UPDATE FROM " + PROFILE_ENTITY
                  + " SET address.street = 'New' + ' Street', SET address.number = 42 WHERE name = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      Address address = profileCache.get(1).address;
      assertThat(address.street).isEqualTo("New Street");
      assertThat(address.number).isEqualTo(42);
   }

   // --- ADD/REMOVE on a collection inside the nested object ---

   @Test
   public void testAddToNestedCollectionWithExpression() {
      Query<Profile> update = profileCache.query(
            "UPDATE FROM " + PROFILE_ENTITY + " ADD address.tags = 'ne' + 'w' WHERE name = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(profileCache.get(1).address.tags).containsExactly("old", "tag", "new");
   }

   @Test
   public void testRemoveFromNestedCollectionWithExpression() {
      Query<Profile> update = profileCache.query(
            "UPDATE FROM " + PROFILE_ENTITY + " REMOVE address.tags = 'ol' + 'd' WHERE name = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(profileCache.get(1).address.tags).containsExactly("tag");
   }

   // --- the same statements with only constant values: the legacy path ---

   @Test
   public void testAddToNestedCollectionWithConstant() {
      Query<Profile> update = profileCache.query(
            "UPDATE FROM " + PROFILE_ENTITY + " ADD address.tags = 'plain' WHERE name = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(profileCache.get(1).address.tags).containsExactly("old", "tag", "plain");
   }

   @Test
   public void testRemoveFromNestedCollectionWithConstant() {
      Query<Profile> update = profileCache.query(
            "UPDATE FROM " + PROFILE_ENTITY + " REMOVE address.tags = 'tag' WHERE name = 'Tom'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(profileCache.get(1).address.tags).containsExactly("old");
   }

   // --- failure behaviour ---

   @Test
   public void testMissingIntermediateFailsLoudly() {
      // the nested object is absent: the statement must fail with an error
      // instead of silently writing nothing
      profileCache.put(2, makeProfileWithoutAddress());
      assertThatThrownBy(() -> profileCache.query(
                  "UPDATE FROM " + PROFILE_ENTITY + " SET address.street = 'x' + '' WHERE name = 'Adrian'")
            .executeStatement())
            .isInstanceOf(RuntimeException.class);
      Profile profile = profileCache.get(2);
      assertThat(profile.address).isNull();
      assertThat(profile.name).isEqualTo("Adrian");
   }

   @Test
   public void testNestedRollbackOnPartialFailure() {
      // the first two operations apply (one top-level, one nested); the third
      // fails at apply time because 'abc' cannot be converted to int. The
      // applied mutations must be rolled back so the entry is unchanged.
      assertThatThrownBy(() -> profileCache.query(
                  "UPDATE FROM " + PROFILE_ENTITY
                        + " SET name = 'changed', SET address.street = 'changed', SET address.number = tostring('abc') WHERE name = 'Tom'")
            .executeStatement())
            .isInstanceOf(RuntimeException.class);
      Profile profile = profileCache.get(1);
      assertThat(profile.name).isEqualTo("Tom");
      assertThat(profile.address.street).isEqualTo("Old Street");
      assertThat(profile.address.number).isEqualTo(7);
      assertThat(profile.address.tags).containsExactly("old", "tag");
   }

   @Test
   public void testCollectionMutationRolledBackOnPartialFailure() {
      // the collection mutation of the first operation must be rolled back
      // when the second operation fails at apply time
      assertThatThrownBy(() -> profileCache.query(
                  "UPDATE FROM " + PROFILE_ENTITY
                        + " ADD address.tags = 'extra', SET address.number = tostring('abc') WHERE name = 'Tom'")
            .executeStatement())
            .isInstanceOf(RuntimeException.class);
      assertThat(profileCache.get(1).address.tags).containsExactly("old", "tag");
   }
}
