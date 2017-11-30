package org.infinispan.server.test.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.AbstractJBossMarshaller;
import org.infinispan.commons.marshall.jboss.DefaultContextClassResolver;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.server.test.category.Queries;
import org.infinispan.server.test.util.ITestUtils;
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
 * Tests for remote query using jboss marshalling and Hibernate Search annotated classes. Protobuf is not used!
 *
 * @author anistor@redhat.com
 * @since 9.1
 */
@RunWith(Arquillian.class)
@Category(Queries.class)
public class RemoteQueryCompatModeIT {

   /**
    * The deployments to cleanup after suite.
    */
   private static final Set<File> deployments = new HashSet<>();

   private static RemoteCacheManager remoteCacheManager;

   @InfinispanResource("custom-compat-marshaller")
   RemoteInfinispanServer server1;

   @BeforeClass
   public static void before() throws Exception {
      // We put the entity class in one jar and the marshaller in another jar, just to make things more interesting.
      JavaArchive entityArchive = ShrinkWrap.create(JavaArchive.class)
            .addClasses(TestEntity.class)
            .add(new StringAsset("Dependencies: org.hibernate.search.engine"), "META-INF/MANIFEST.MF"); // we need the Hibernate Search annotations

      JavaArchive marshallerArchive = ShrinkWrap.create(JavaArchive.class)
            .addClasses(CustomCompatModeMarshaller.class)
            .add(new StringAsset("Dependencies: org.jboss.marshalling, " +
                        "org.infinispan.commons, " +
                        "org.infinispan.remote-query.client, " +
                        "deployment.custom-test-entity.jar"),  // We depend on the jar containing the entity, so we can instantiate it.
                  "META-INF/MANIFEST.MF")
            .addAsServiceProvider(Marshaller.class, CustomCompatModeMarshaller.class);

      File deployment1 = new File(System.getProperty("server1.dist"), "/standalone/deployments/custom-test-entity.jar");
      entityArchive.as(ZipExporter.class).exportTo(deployment1, true);
      deployments.add(deployment1);
      File deployment2 = new File(System.getProperty("server1.dist"), "/standalone/deployments/custom-compat-marshaller.jar");
      marshallerArchive.as(ZipExporter.class).exportTo(deployment2, true);
      deployments.add(deployment2);
   }

   @AfterClass
   public static void after() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
      for (File f : deployments) {
         f.delete();
      }
   }

   @Test
   @WithRunningServer(@RunningServer(name = "custom-compat-marshaller"))
   public void testCompatQuery() throws Exception {
      remoteCacheManager = ITestUtils.createCacheManager(server1);
      RemoteCache<Integer, TestEntity> remoteCache = remoteCacheManager.getCache();
      remoteCache.clear();

      for (int i = 0; i < 10; i++) {
         TestEntity user = new TestEntity("name" + i);
         remoteCache.put(i, user);
      }

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);

      Query query = queryFactory.from(TestEntity.class).build();

      List<?> list = query.list();
      assertEquals(10, list.size());
      assertTrue(list.get(0) instanceof TestEntity);
   }

   public static final class CustomCompatModeMarshaller extends AbstractJBossMarshaller {

      public CustomCompatModeMarshaller() {
         baseCfg.setClassResolver(new DefaultContextClassResolver(getClass().getClassLoader()));
      }
   }
}
