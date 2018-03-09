package org.infinispan.server.test.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Date;
import java.util.List;

import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.snowball.SnowballPorterFilterFactory;
import org.apache.lucene.analysis.standard.StandardFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.sampledomain.Transaction;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.infinispan.server.test.category.Queries;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test for full-text remote queries over HotRod using Ickle query language. A ProgrammaticSearchMappingProvider is
 * used to define the analyzers.
 *
 * @author anistor@redhat.com
 * @since 9.2
 */
@RunWith(Arquillian.class)
@Category(Queries.class)
public class RemoteQueryStringIT {

   private static final String TEST_PROGRAMMATIC_SEARCH_MAPPING_PROVIDER_JAR = "test-ProgrammaticSearchMappingProvider.jar";

   private static File deployment;

   private static RemoteCacheManager remoteCacheManager;

   @InfinispanResource("query-programmatic-search-mapping-provider")
   RemoteInfinispanServer server1;

   @BeforeClass
   public static void before() {
      JavaArchive programmaticSearchMappingProviderArchive = ShrinkWrap.create(JavaArchive.class)
            .addClass(TestAnalyzerProvider.class)
            .addClass(TestSearchMappingFactory.class)
            .addClass(TestSearchMappingFactory.MySearchableEntity.class)
            .add(new StringAsset("Dependencies: org.infinispan.query, org.hibernate.search.engine"), "META-INF/MANIFEST.MF")
            .addAsServiceProvider(ProgrammaticSearchMappingProvider.class, TestAnalyzerProvider.class);

      deployment = new File(System.getProperty("server1.dist"), "/standalone/deployments/" + TEST_PROGRAMMATIC_SEARCH_MAPPING_PROVIDER_JAR);
      programmaticSearchMappingProviderArchive.as(ZipExporter.class).exportTo(deployment, true);
   }

   @AfterClass
   public static void after() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
      if (deployment != null) {
         deployment.delete();
      }
   }

   @Test
   @WithRunningServer(@RunningServer(name = "query-programmatic-search-mapping-provider"))
   public void testFullTextTermRightOperandAnalyzed() throws Exception {
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort())
            .marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));

      RemoteCache<Integer, Transaction> remoteCache = remoteCacheManager.getCache();
      remoteCache.clear();

      remoteCache.put(1, createTransaction1());

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query q = qf.create("from sample_bank_account.Transaction where longDescription:'RENT'");

      List<Transaction> list = q.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Transaction.class, list.get(0).getClass());
      assertTransaction1(list.get(0));
   }

   private Transaction createTransaction1() {
      Transaction tx = new Transaction();
      tx.setId(1);
      tx.setAccountId(777);
      tx.setAmount(500);
      tx.setDate(new Date(1));
      tx.setDescription("February rent");
      tx.setLongDescription("February rent");
      tx.setNotes("card was not present");
      return tx;
   }

   private void assertTransaction1(Transaction tx) {
      assertNotNull(tx);
      assertEquals(1, tx.getId());
      assertEquals(777, tx.getAccountId());
      assertEquals(500, tx.getAmount(), 0);
      assertEquals(new Date(1), tx.getDate());
      assertEquals("February rent", tx.getDescription());
      assertEquals("February rent", tx.getLongDescription());
      assertEquals("card was not present", tx.getNotes());
   }

   /**
    * This is a service provider present in META-INF of test-ProgrammaticSearchMappingProvider.jar. This deployment is
    * also listed in the 'modules' element of the cache container, so the provider can be loaded.
    */
   public static final class TestAnalyzerProvider implements ProgrammaticSearchMappingProvider {

      @Override
      public void defineMappings(Cache cache, SearchMapping searchMapping) {
         searchMapping
               .analyzerDef("standard", StandardTokenizerFactory.class)
                  .filter(StandardFilterFactory.class)
                  .filter(LowerCaseFilterFactory.class)
                  .filter(StopFilterFactory.class)
               .analyzerDef("stemmer", StandardTokenizerFactory.class)
                  .filter(StandardFilterFactory.class)
                  .filter(LowerCaseFilterFactory.class)
                  .filter(StopFilterFactory.class)
                  .filter(SnowballPorterFilterFactory.class)
                     .param("language", "English")
               .analyzerDef("ngram", StandardTokenizerFactory.class)
                  .filter(StandardFilterFactory.class)
                  .filter(LowerCaseFilterFactory.class)
                  .filter(StopFilterFactory.class)
                  .filter(NGramFilterFactory.class)
                     .param("minGramSize", "3")
                     .param("maxGramSize", "3");
         }
   }
}
