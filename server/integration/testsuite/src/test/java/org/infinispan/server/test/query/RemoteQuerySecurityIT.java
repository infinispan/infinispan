package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.category.Queries;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.security.SaslConfigurationBuilder;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.ADMIN_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.ADMIN_PASSWD;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.READER_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.READER_PASSWD;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.WRITER_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.WRITER_PASSWD;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.SUPERVISOR_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.SUPERVISOR_PASSWD;

import static org.junit.Assert.*;

/**
 * Tests for remote queries over HotRod with security on a DIST indexed/non-indexed cache.
 *
 * @author Adrian Nistor
 * @since 7.2
 */
@Category({Queries.class})
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "remote-query-security")})
public class RemoteQuerySecurityIT {

   @InfinispanResource("remote-query-security")
   protected RemoteInfinispanServer server;

   private RemoteCacheManagerFactory rcmFactory;
   private Map<String, RemoteCacheManager> remoteCacheManagers = new HashMap<String, RemoteCacheManager>();

   private static final String TEST_CACHE_INDEXED = "test_cache_indexed";
   private static final String TEST_CACHE_NOT_INDEXED = "test_cache_not_indexed";

   private static final String TEST_SERVER_NAME = "node2";
   private static final String SASL_MECH = "PLAIN";

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   @Before
   public void setUp() throws Exception {
      rcmFactory = new RemoteCacheManagerFactory();
      remoteCacheManagers.put(ADMIN_LOGIN, rcmFactory.createManager(getClientConfigBuilderForUser(ADMIN_LOGIN, ADMIN_PASSWD)));
      remoteCacheManagers.put(WRITER_LOGIN, rcmFactory.createManager(getClientConfigBuilderForUser(WRITER_LOGIN, WRITER_PASSWD)));
      remoteCacheManagers.put(READER_LOGIN, rcmFactory.createManager(getClientConfigBuilderForUser(READER_LOGIN, READER_PASSWD)));
      remoteCacheManagers.put(SUPERVISOR_LOGIN, rcmFactory.createManager(getClientConfigBuilderForUser(SUPERVISOR_LOGIN, SUPERVISOR_PASSWD)));

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManagers.get(ADMIN_LOGIN).getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      String proto = Util.read(getClass().getResourceAsStream("/sample_bank_account/bank.proto"));
      metadataCache.put("sample_bank_account/bank.proto", proto);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManagers.get(ADMIN_LOGIN)));
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManagers.get(READER_LOGIN)));
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManagers.get(WRITER_LOGIN)));
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManagers.get(SUPERVISOR_LOGIN)));

      User user = new User();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singleton(12));

      remoteCacheManagers.get(ADMIN_LOGIN).getCache(TEST_CACHE_INDEXED).put(1, user);
      remoteCacheManagers.get(ADMIN_LOGIN).getCache(TEST_CACHE_NOT_INDEXED).put(1, user);
   }

   private ConfigurationBuilder getClientConfigBuilderForUser(String login, String password) {
      return new SaslConfigurationBuilder(SASL_MECH)
            .forIspnServer(server)
            .withServerName(TEST_SERVER_NAME)
            .forCredentials(login, password)
            .marshaller(new ProtoStreamMarshaller());
   }

   @After
   public void tearDown() {
      if (rcmFactory != null) {
         rcmFactory.stopManagers();
      }
      rcmFactory = null;
   }

   @Test
   public void testReaderQueryIndexed() {
      expectedException.expect(HotRodClientException.class);
      expectedException.expectMessage("Unauthorized access");

      execQuery(READER_LOGIN, TEST_CACHE_INDEXED);
   }

   @Test
   public void testReaderQueryNotIndexed() {
      expectedException.expect(HotRodClientException.class);
      expectedException.expectMessage("Unauthorized access");

      execQuery(READER_LOGIN, TEST_CACHE_NOT_INDEXED);
   }

   @Test
   public void testWriterQueryIndexed() {
      expectedException.expect(HotRodClientException.class);
      expectedException.expectMessage("Unauthorized access");

      execQuery(WRITER_LOGIN, TEST_CACHE_INDEXED);
   }

   @Test
   public void testWriterQueryNotIndexed() {
      expectedException.expect(HotRodClientException.class);
      expectedException.expectMessage("Unauthorized access");

      execQuery(WRITER_LOGIN, TEST_CACHE_NOT_INDEXED);
   }

   @Test
   public void testSupervisorQueryIndexed() {
      execQuery(SUPERVISOR_LOGIN, TEST_CACHE_INDEXED);
   }

   @Test
   public void testSupervisorQueryNotIndexed() {
      execQuery(SUPERVISOR_LOGIN, TEST_CACHE_NOT_INDEXED);
   }

   @Test
   public void testAdminQueryIndexed() {
      execQuery(ADMIN_LOGIN, TEST_CACHE_INDEXED);
   }

   @Test
   public void testAdminQueryNotIndexed() {
      execQuery(ADMIN_LOGIN, TEST_CACHE_NOT_INDEXED);
   }

   private void execQuery(String userLogin, String cacheName) {
      RemoteCache<Object, Object> cache = remoteCacheManagers.get(userLogin).getCache(cacheName);
      QueryFactory qf = Search.getQueryFactory(cache);
      Query query = qf.from(User.class)
            .having("name").eq("Tom").toBuilder()
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(User.class, list.get(0).getClass());
   }
}
