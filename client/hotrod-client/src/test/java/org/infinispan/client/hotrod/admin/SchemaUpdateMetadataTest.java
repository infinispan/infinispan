package org.infinispan.client.hotrod.admin;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.query.testdomain.protobuf.Programmer;
import org.infinispan.client.hotrod.query.testdomain.protobuf.ProgrammerSchemaImpl;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.ObjectPropertyHelper;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.impl.ResourceUtils;
import org.infinispan.query.core.impl.QueryCache;
import org.infinispan.query.model.Developer;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.schema.SchemaUpdateMetadataTest")
@TestForIssue(jiraKey = "ISPN-14527")
public class SchemaUpdateMetadataTest extends SingleHotRodServerTest {

   public static final ProgrammerSchemaImpl PROGRAMMER_SCHEMA = new ProgrammerSchemaImpl();
   public static final String QUERY_SORT = "from io.pro.Programmer p order by p.contributions";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("io.pro.Programmer");
      return TestCacheManagerFactory.createServerModeCacheManager(contextInitializer(), config);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return PROGRAMMER_SCHEMA;
   }

   /**
    * Configure the server, enabling the admin operations
    *
    * @return the HotRod server
    */
   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());

      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Test
   public void test() throws Exception {
      RemoteCache<String, Programmer> remoteCache = remoteCacheManager.getCache();

      cache.put(1, new Programmer("fax4ever", 300));

      Query<Developer> kQuery;
      QueryResult<Developer> kResult;

      kQuery = remoteCache.query(QUERY_SORT);
      kResult = kQuery.execute();
      assertThat(kResult.count().value()).isEqualTo(1);

      verifySortable(false);
      queryIsOnTheCache(true);

      updateTheSchemaAndReindex();

      verifySortable(true);
      queryIsOnTheCache(false);

      kQuery = remoteCache.query(QUERY_SORT);
      kResult = kQuery.execute();
      assertThat(kResult.count().value()).isEqualTo(1);

      queryIsOnTheCache(true);
   }

   @Test
   public void testSchemaReplace() {
      RemoteCache<String, String> protobufMetadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      protobufMetadataCache.putIfAbsent(PROGRAMMER_SCHEMA.getProtoFileName(), PROGRAMMER_SCHEMA.getProtoFile());

      var existing = protobufMetadataCache.getWithMetadata(PROGRAMMER_SCHEMA.getProtoFileName());

      // should succeed, version match
      assertTrue(protobufMetadataCache.replaceWithVersion(PROGRAMMER_SCHEMA.getProtoFileName(), PROGRAMMER_SCHEMA.getProtoFile(), existing.getVersion()));

      // should fail, version should not match
      assertFalse(protobufMetadataCache.replaceWithVersion(PROGRAMMER_SCHEMA.getProtoFileName(), "should-never-be-parser", existing.getVersion()));

      // should succeed
      assertTrue(protobufMetadataCache.replace(PROGRAMMER_SCHEMA.getProtoFileName(), PROGRAMMER_SCHEMA.getProtoFile(), PROGRAMMER_SCHEMA.getProtoFile()));

      // should fail, old value is not equals
      assertFalse(protobufMetadataCache.replace(PROGRAMMER_SCHEMA.getProtoFileName(), "for-equals-to-fail", "should-never-be-parser"));
   }

   private void queryIsOnTheCache(boolean isPresent) {
      QueryCache queryCache = getGlobalQueryCache();
      AtomicBoolean present = new AtomicBoolean(true);
      queryCache.get(cache.getName(), QUERY_SORT, null, IckleParsingResult.class, (qs, accumulators) -> {
         present.set(false); // true => is not present
         return null;
      });
      assertThat(present.get()).isEqualTo(isPresent);
   }

   private QueryCache getGlobalQueryCache() {
      return ComponentRegistry.componentOf(cache, QueryCache.class);
   }

   private void updateTheSchemaAndReindex() {
      String newProtoFile = ResourceUtils.getResourceAsString(getClass(), "/proto/pro-sortable.proto");
      cacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME)
            .put(PROGRAMMER_SCHEMA.getProtoFileName(), newProtoFile);
      remoteCacheManager.administration().reindexCache(cache.getName());
   }

   private void verifySortable(boolean expected) {
      Descriptor descriptor = descriptor();
      FieldDescriptor fieldDescriptor = descriptor.findFieldByName("contributions");
      Map<String, AnnotationElement.Annotation> annotations = fieldDescriptor.getAnnotations();
      assertThat(annotations).containsKey("Basic");

      AnnotationElement.Annotation basic = annotations.get("Basic");
      AnnotationElement.Value sortable = basic.getAttributeValue("sortable");
      assertThat(sortable.toString()).isEqualTo(expected + "");
   }

   private Descriptor descriptor() {
      ProtobufMatcher matcher = ComponentRegistry.componentOf(cache, ProtobufMatcher.class);
      assertThat(matcher).isNotNull();

      ObjectPropertyHelper<Descriptor> propertyHelper = matcher.getPropertyHelper();
      assertThat(propertyHelper).isNotNull();

      Descriptor descriptor = propertyHelper.getEntityMetadata("io.pro.Programmer");
      assertThat(descriptor).isNotNull();

      return descriptor;
   }
}
