package org.infinispan.query.core.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.Address;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * UPDATE statements on a protobuf-encoded cache (the encoding used by Hot Rod
 * and remote caches), covering the two protobuf-specific apply paths:
 * nested property paths (the leaf mutation must be propagated through the
 * intermediate messages) and fields of message type (the value is serialized
 * to protobuf and stored as raw nested-message bytes).
 * <p>
 * A statement with a value expression must behave exactly like the same
 * statement with a plain constant value.
 */
@Test(groups = "functional", testName = "query.core.tests.NestedUpdatePathTest")
public class NestedUpdatePathTest extends SingleCacheManagerTest {

   private static final String USER_ENTITY = User.class.getName();

   private Cache<Integer, User> userCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
            .indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(User.class);

      cacheManager = TestCacheManagerFactory.createCacheManager(TestDomainSCI.INSTANCE, null);
      userCache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("nested-update-path", builder.build());
      return cacheManager;
   }

   @BeforeMethod
   public void populate() {
      userCache.clear();
      userCache.put(1, makeUser());
   }

   private static User makeUser() {
      User user = new User();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      Address address = new Address();
      address.setStreet("Old Street");
      address.setPostCode("1234");
      address.setNumber(7);
      user.setAddresses(new ArrayList<>(List.of(address)));
      return user;
   }

   private static User makeUserWithoutAddresses() {
      User user = new User();
      user.setId(2);
      user.setName("Adrian");
      user.setSurname("Nistor");
      return user;
   }

   // --- SET with a value expression on a nested path ---

   @Test
   public void testSetNestedStringFieldWithExpression() {
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " SET addresses.street = 'New' + ' Street' WHERE id = 1");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(userCache.get(1).getAddresses().get(0).getStreet()).isEqualTo("New Street");
   }

   @Test
   public void testSetNestedNumericFieldWithExpression() {
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " SET addresses.number = addresses.number + 1 WHERE id = 1");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(userCache.get(1).getAddresses().get(0).getNumber()).isEqualTo(8);
   }

   @Test
   public void testSetNestedFieldFromTopLevelProperty() {
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " SET addresses.street = surname WHERE id = 1");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(userCache.get(1).getAddresses().get(0).getStreet()).isEqualTo("Cat");
   }

   @Test
   public void testMultipleOperationsOnTheSameNestedMessage() {
      // both operations descend into the same nested message: the second one
      // must re-read the bytes written by the first
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY
                  + " SET addresses.street = 'New' + ' Street', SET addresses.number = addresses.number + 35 WHERE id = 1");
      assertThat(update.executeStatement()).isEqualTo(1);
      Address address = userCache.get(1).getAddresses().get(0);
      assertThat(address.getStreet()).isEqualTo("New Street");
      assertThat(address.getNumber()).isEqualTo(42);
   }

   // --- REMOVE with a value expression on a nested path ---

   @Test
   public void testRemoveNestedFieldWithExpression() {
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " REMOVE addresses.number = 6 + 1 WHERE id = 1");
      assertThat(update.executeStatement()).isEqualTo(1);
      // removing the value clears the field, so it reads back as the default
      assertThat(userCache.get(1).getAddresses().get(0).getNumber()).isZero();
   }

   @Test
   public void testRemoveNestedFieldWithConstant() {
      // the same statement with a plain constant value: must behave like the expression form above
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " REMOVE addresses.number = 7 WHERE id = 1");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(userCache.get(1).getAddresses().get(0).getNumber()).isZero();
   }

   // --- missing intermediate message ---

   @Test
   public void testSetNestedFieldCreatesMissingIntermediate() {
      userCache.put(2, makeUserWithoutAddresses());
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " SET addresses.street = 'Created' WHERE id = 2");
      assertThat(update.executeStatement()).isEqualTo(1);
      User user = userCache.get(2);
      assertThat(user.getAddresses()).hasSize(1);
      assertThat(user.getAddresses().get(0).getStreet()).isEqualTo("Created");
   }

   @Test
   public void testSetNestedFieldWithExpressionCreatesMissingIntermediate() {
      userCache.put(2, makeUserWithoutAddresses());
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " SET addresses.street = 'Crea' + 'ted' WHERE id = 2");
      assertThat(update.executeStatement()).isEqualTo(1);
      User user = userCache.get(2);
      assertThat(user.getAddresses()).hasSize(1);
      assertThat(user.getAddresses().get(0).getStreet()).isEqualTo("Created");
   }

   // --- fields of message type ---

   @Test
   public void testSetMessageValuedFieldWithParameter() {
      // SET on a field of message type: the parameter is serialized to protobuf
      // and replaces the whole field
      Address replacement = new Address();
      replacement.setStreet("Parameter Street");
      replacement.setPostCode("5678");
      replacement.setNumber(42);

      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " SET addresses = :newAddress WHERE id = 1");
      update.setParameter("newAddress", replacement);
      assertThat(update.executeStatement()).isEqualTo(1);

      List<Address> addresses = userCache.get(1).getAddresses();
      assertThat(addresses).hasSize(1);
      assertThat(addresses.get(0).getStreet()).isEqualTo("Parameter Street");
      assertThat(addresses.get(0).getPostCode()).isEqualTo("5678");
      assertThat(addresses.get(0).getNumber()).isEqualTo(42);
   }

   @Test
   public void testAddMessageValuedFieldElement() {
      Address extra = new Address();
      extra.setStreet("Second Street");
      extra.setPostCode("5679");
      extra.setNumber(9);

      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " ADD addresses = :extraAddress WHERE id = 1");
      update.setParameter("extraAddress", extra);
      assertThat(update.executeStatement()).isEqualTo(1);

      List<Address> addresses = userCache.get(1).getAddresses();
      assertThat(addresses).hasSize(2);
      assertThat(addresses.get(0).getStreet()).isEqualTo("Old Street");
      assertThat(addresses.get(1).getStreet()).isEqualTo("Second Street");
   }

   @Test
   public void testRemoveMessageValuedFieldElement() {
      User user = makeUser();
      Address second = new Address();
      second.setStreet("Second Street");
      second.setPostCode("5679");
      second.setNumber(9);
      user.getAddresses().add(second);
      userCache.put(1, user);

      // the parameter equals the first address by value: only that one is removed
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " REMOVE addresses = :oldAddress WHERE id = 1");
      update.setParameter("oldAddress", makeUser().getAddresses().get(0));
      assertThat(update.executeStatement()).isEqualTo(1);

      List<Address> addresses = userCache.get(1).getAddresses();
      assertThat(addresses).hasSize(1);
      assertThat(addresses.get(0).getStreet()).isEqualTo("Second Street");
   }

   // --- controls: the same statements without expressions, and a top-level path ---

   @Test
   public void testSetNestedFieldWithConstant() {
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " SET addresses.street = 'Plain Street' WHERE id = 1");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(userCache.get(1).getAddresses().get(0).getStreet()).isEqualTo("Plain Street");
   }

   @Test
   public void testSetTopLevelFieldWithExpression() {
      Query<User> update = userCache.query(
            "UPDATE FROM " + USER_ENTITY + " SET name = 'New' + ' Name' WHERE id = 1");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(userCache.get(1).getName()).isEqualTo("New Name");
   }
}
