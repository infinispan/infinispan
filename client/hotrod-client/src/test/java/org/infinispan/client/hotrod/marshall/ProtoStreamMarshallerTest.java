package org.infinispan.client.hotrod.marshall;

import com.google.protobuf.Descriptors;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.marshall.ProtostreamMarshaller;
import org.infinispan.client.hotrod.protostream.domain.Account;
import org.infinispan.client.hotrod.protostream.domain.Address;
import org.infinispan.client.hotrod.protostream.domain.Transaction;
import org.infinispan.client.hotrod.protostream.domain.User;
import org.infinispan.client.hotrod.protostream.domain.marshallers.AccountMarshaller;
import org.infinispan.client.hotrod.protostream.domain.marshallers.AddressMarshaller;
import org.infinispan.client.hotrod.protostream.domain.marshallers.GenderEncoder;
import org.infinispan.client.hotrod.protostream.domain.marshallers.TransactionMarshaller;
import org.infinispan.client.hotrod.protostream.domain.marshallers.UserMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests integration between HotRod client and ProtoStream marshalling library.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.protostream.ProtoStreamMarshallerTest", groups = "functional")
public class ProtoStreamMarshallerTest extends SingleCacheManagerTest {

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, User> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      initSerializationContext();

      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      Properties hotrodClientConf = new Properties();
      hotrodClientConf.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer.getPort());
      hotrodClientConf.put("infinispan.client.hotrod.marshaller", ProtostreamMarshaller.class.getName());
      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   @Test
   public void testPutAndGet() throws Exception {
      User user = new User();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singletonList(12));
      Address address = new Address();
      address.setStreet("Dark Alley");
      address.setPostCode("1234");
      user.setAddresses(Collections.singletonList(address));

      remoteCache.put(1, user);

      User fromCache = remoteCache.get(1);
      assertEquals(1, fromCache.getId());
      assertEquals("Tom", fromCache.getName());
      assertEquals("Cat", fromCache.getSurname());
      assertEquals(User.Gender.MALE, fromCache.getGender());
      assertNotNull(fromCache.getAccountIds());
      assertEquals(1, fromCache.getAccountIds().size());
      assertEquals(12, fromCache.getAccountIds().get(0).intValue());
      assertNotNull(fromCache.getAddresses());
      assertEquals(1, fromCache.getAddresses().size());
      assertEquals("Dark Alley", fromCache.getAddresses().get(0).getStreet());
      assertEquals("1234", fromCache.getAddresses().get(0).getPostCode());
   }

   private SerializationContext initSerializationContext() throws IOException, Descriptors.DescriptorValidationException {
      SerializationContext ctx = ProtostreamMarshaller.getSerializationContext();
      ctx.registerProtofile("/bank.protobin");
      ctx.registerMarshaller(User.class, new UserMarshaller());
      ctx.registerEnumEncoder(User.Gender.class, new GenderEncoder());
      ctx.registerMarshaller(Address.class, new AddressMarshaller());
      ctx.registerMarshaller(Account.class, new AccountMarshaller());
      ctx.registerMarshaller(Transaction.class, new TransactionMarshaller());
      return ctx;
   }
}
