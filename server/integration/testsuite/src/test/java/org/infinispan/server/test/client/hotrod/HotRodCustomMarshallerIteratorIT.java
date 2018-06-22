package org.infinispan.server.test.client.hotrod;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DEPLOYMENT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.READ_ATTRIBUTE_OPERATION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.STATUS;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
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
import org.infinispan.query.remote.client.ProtostreamSerializationContextInitializer;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.dmr.ModelNode;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for remote iteration using a custom filter with custom classes marshalled with a custom Protobuf based
 * marshaller.
 *
 * @author gustavonalle
 * @author anistor@redhat.com
 * @since 8.0
 */
@RunWith(Arquillian.class)
@WithRunningServer(@RunningServer(name = "remote-iterator-local"))
public class HotRodCustomMarshallerIteratorIT {

   private static final String FILTER_MARSHALLER_DEPLOYMENT_JAR = "filter-marshaller.jar";
   private static final String TO_STRING_FILTER_CONVERTER_FACTORY_NAME = "to-string-filter-converter";
   private static final String PARAM_FILTER_CONVERTER_FACTORY_NAME = "param-filter-converter";
   private static final String CACHE_NAME = "default";

   private static RemoteCacheManager remoteCacheManager;

   private RemoteCache<Integer, User> remoteCache;

   @InfinispanResource("remote-iterator-local")
   RemoteInfinispanServer server1;

   @BeforeClass
   public static void deploy() throws IOException {
      String protoFile = Util.getResourceAsString("/sample_bank_account/bank.proto", HotRodCustomMarshallerIteratorIT.class.getClassLoader());

      JavaArchive archive = ShrinkWrap.create(JavaArchive.class, FILTER_MARSHALLER_DEPLOYMENT_JAR)
            // Add custom marshaller classes
            .addClasses(HotRodClientException.class, UserMarshaller.class, GenderMarshaller.class, User.class, Address.class)
            // Add marshaller dependencies
            .add(new StringAsset(protoFile), "/sample_bank_account/bank.proto")
            .add(new StringAsset("Dependencies: org.infinispan.protostream, org.infinispan.remote-query.client"), "META-INF/MANIFEST.MF")
            .addClass(ServerCtxInitializer.class)
            .addAsServiceProvider(ProtostreamSerializationContextInitializer.class, ServerCtxInitializer.class)
            // Add custom filterConverter classes
            .addClasses(CustomFilterFactory.class, CustomFilterFactory.CustomFilter.class, ParamCustomFilterFactory.class,
                  ParamCustomFilterFactory.ParamCustomFilter.class)
            // Register custom filterConverterFactories
            .addAsServiceProviderAndClasses(KeyValueFilterConverterFactory.class, ParamCustomFilterFactory.class, CustomFilterFactory.class);

      File deployment = new File(System.getProperty("server1.dist"), "/standalone/deployments/" + FILTER_MARSHALLER_DEPLOYMENT_JAR);
      archive.as(ZipExporter.class).exportTo(deployment, true);
   }

   @Before
   public void setup() throws Exception {
      RemoteCacheManagerFactory remoteCacheManagerFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort())
            .marshaller(new CustomProtoStreamMarshaller());
      remoteCacheManager = remoteCacheManagerFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);

      waitForDeploymentCompletion();
   }

   /**
    * Waits up to one minute until the jar deployment completes successfully, namely invocation of DMR operation
    * "/deployment=filter-marshaller.jar/:read-attribute(name=status)" yields "success".
    */
   private void waitForDeploymentCompletion() throws Exception {
      ModelControllerClient controllerClient = ModelControllerClient.Factory.create(server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);
      PathAddress address = PathAddress.pathAddress(PathElement.pathElement(DEPLOYMENT, FILTER_MARSHALLER_DEPLOYMENT_JAR));
      ModelNode op = new ModelNode();
      op.get(OP).set(READ_ATTRIBUTE_OPERATION);
      op.get(OP_ADDR).set(address.toModelNode());
      op.get(NAME).set(STATUS);
      eventually(() -> SUCCESS.equals(controllerClient.execute(op).get(OUTCOME).asString()), 60000);
   }

   @AfterClass
   public static void after() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
      new File(System.getProperty("server1.dist"), "/standalone/deployments/" + FILTER_MARSHALLER_DEPLOYMENT_JAR).delete();
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
         assertNotNull(iterator);
         iterator.close();
      }
      return entryMap;
   }

   public static class ServerCtxInitializer implements ProtostreamSerializationContextInitializer {

      @Override
      public void init(SerializationContext serializationContext) throws IOException {
         serializationContext.registerProtoFiles(FileDescriptorSource.fromResources(ServerCtxInitializer.class.getClassLoader(), "/sample_bank_account/bank.proto"));
         serializationContext.registerMarshaller(new UserMarshaller());
         serializationContext.registerMarshaller(new GenderMarshaller());
      }
   }

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
