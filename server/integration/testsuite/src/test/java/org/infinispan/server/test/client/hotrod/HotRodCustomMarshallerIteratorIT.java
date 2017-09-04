package org.infinispan.server.test.client.hotrod;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Util;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.GenderMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.UserMarshaller;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;
import org.infinispan.server.test.category.HotRodSingleNode;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for remote iteration using a custom filter with custom classes marshalled with a custom Protobuf based marshaller.
 *
 * @author gustavonalle
 * @since 8.0
 */
@RunWith(Arquillian.class)
@Category(HotRodSingleNode.class)
@Ignore("ISPN-8001")
public class HotRodCustomMarshallerIteratorIT {

   private static final String TO_STRING_FILTER_CONVERTER_FACTORY_NAME = "to-string-filter-converter";
   private static final String PARAM_FILTER_CONVERTER_FACTORY_NAME = "param-filter-converter";
   private static final String CACHE_NAME = "default";
   private static RemoteCacheManager remoteCacheManager;

   private RemoteCache<Integer, User> remoteCache;

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @Deployment(testable = false, name = "marshallerFilter")
   @TargetsContainer("container1")
   public static Archive<?> deploy() throws IOException {
      return createFilterMarshallerArchive();
   }

   @Before
   public void setup() throws IOException {
      RemoteCacheManagerFactory remoteCacheManagerFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
                   .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
                   .port(server1.getHotrodEndpoint().getPort())
                   .marshaller(new CustomProtoStreamMarshaller());
      remoteCacheManager = remoteCacheManagerFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
   }

   @AfterClass
   public static void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }

   private static Archive<?> createFilterMarshallerArchive() throws IOException {
      String protoFile = Util.getResourceAsString("/sample_bank_account/bank.proto", HotRodCustomMarshallerIteratorIT.class.getClassLoader());

      return ShrinkWrap.create(JavaArchive.class, "filter-marshaller.jar")
            // Add custom marshaller classes
            .addClasses(CustomProtoStreamMarshaller.class, ProtoStreamMarshaller.class, BaseProtoStreamMarshaller.class)
            .addClasses(HotRodClientException.class, UserMarshaller.class, GenderMarshaller.class, User.class, Address.class)
            // Add marshaller dependencies
            .add(new StringAsset(protoFile), "/sample_bank_account/bank.proto")
            .add(new StringAsset("Dependencies: org.infinispan.protostream"), "META-INF/MANIFEST.MF")
            // Register marshaller
            .addAsServiceProvider(Marshaller.class, CustomProtoStreamMarshaller.class)
            // Add custom filterConverter classes
            .addClasses(CustomFilterFactory.class, CustomFilterFactory.CustomFilter.class, ParamCustomFilterFactory.class,
                  ParamCustomFilterFactory.ParamCustomFilter.class)
            // Register custom filterConverterFactories
            .addAsServiceProviderAndClasses(KeyValueFilterConverterFactory.class, ParamCustomFilterFactory.class, CustomFilterFactory.class);
   }

   @Test
   public void testIteration() {
      remoteCache.clear();

      for (int i = 0; i < 10; i++) {
         User user = new User();
         user.setId(i);
         user.setName("name" + i);
         user.setSurname("surname" + i);
         remoteCache.put(i, user);
      }

      // Filter-less iteration
      Map<Object, Object> entryMap = iteratorToMap(remoteCache.retrieveEntries(null, 10));
      assertEquals(10, entryMap.size());
      assertEquals(((User) entryMap.get(2)).getName(), "name2");

      // Iteration with filter
      entryMap = iteratorToMap(remoteCache.retrieveEntries(TO_STRING_FILTER_CONVERTER_FACTORY_NAME, 10));
      assertEquals(10, entryMap.size());
      assertTrue(((String) entryMap.get(2)).startsWith("User{"));

      // Iteration with parametrised filter
      entryMap = iteratorToMap(remoteCache.retrieveEntries(PARAM_FILTER_CONVERTER_FACTORY_NAME, new Object[]{3}, null, 10));
      assertEquals(10, entryMap.size());
      assertTrue(entryMap.get(2).equals("Use"));
   }

   private Map<Object, Object> iteratorToMap(CloseableIterator<Entry<Object, Object>> iterator) {
      Map<Object, Object> entryMap = new HashMap<>();
      try {
         while (iterator.hasNext()) {
            Entry<Object, Object> next = iterator.next();
            entryMap.put(next.getKey(), next.getValue());
         }
      } finally {
         assert iterator != null;
         iterator.close();
      }
      return entryMap;
   }

   // Custom filter and marshaller

   public static class CustomProtoStreamMarshaller extends ProtoStreamMarshaller {
      public CustomProtoStreamMarshaller() throws IOException {
         SerializationContext serCtx = getSerializationContext();
         serCtx.registerProtoFiles(FileDescriptorSource.fromResources(CustomProtoStreamMarshaller.class.getClassLoader(), "/sample_bank_account/bank.proto"));
         serCtx.registerMarshaller(new UserMarshaller());
         serCtx.registerMarshaller(new GenderMarshaller());
      }
   }


   @NamedFactory(name = TO_STRING_FILTER_CONVERTER_FACTORY_NAME)
   public static class CustomFilterFactory implements KeyValueFilterConverterFactory<Integer, User, String> {
      @Override
      public KeyValueFilterConverter<Integer, User, String> getFilterConverter() {
         return new CustomFilter();
      }

      public static class CustomFilter extends AbstractKeyValueFilterConverter<Integer, User, String> implements Serializable {
         @Override
         public String filterAndConvert(Integer key, User value, Metadata metadata) {
            return value.toString();
         }
      }
   }

   @NamedFactory(name = PARAM_FILTER_CONVERTER_FACTORY_NAME)
   public static class ParamCustomFilterFactory implements ParamKeyValueFilterConverterFactory<Integer, User, String> {

      @Override
      public KeyValueFilterConverter<Integer, User, String> getFilterConverter(Object[] params) {
         return new ParamCustomFilter((Integer) params[0]);
      }

      public static class ParamCustomFilter extends AbstractKeyValueFilterConverter<Integer, User, String> implements Serializable {
         private final int maxLength;

         public ParamCustomFilter(int maxLength) {
            this.maxLength = maxLength;
         }

         @Override
         public String filterAndConvert(Integer key, User value, Metadata metadata) {
            return value.toString().substring(0, maxLength);
         }
      }
   }
}
