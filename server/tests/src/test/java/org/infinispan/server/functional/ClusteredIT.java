package org.infinispan.server.functional;

import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.server.functional.extensions.DistributedHelloServerTask;
import org.infinispan.server.functional.extensions.HelloServerTask;
import org.infinispan.server.functional.extensions.IsolatedTask;
import org.infinispan.server.functional.extensions.Person;
import org.infinispan.server.functional.extensions.PojoMarshalling;
import org.infinispan.server.functional.extensions.ScriptingTasks;
import org.infinispan.server.functional.extensions.ServerTasks;
import org.infinispan.server.functional.extensions.SharedTask;
import org.infinispan.server.functional.extensions.entities.Entities;
import org.infinispan.server.functional.extensions.entities.EntitiesImpl;
import org.infinispan.server.functional.extensions.filters.DynamicCacheEventFilterFactory;
import org.infinispan.server.functional.extensions.filters.DynamicConverterFactory;
import org.infinispan.server.functional.extensions.filters.FilterConverterFactory;
import org.infinispan.server.functional.extensions.filters.RawStaticCacheEventFilterFactory;
import org.infinispan.server.functional.extensions.filters.RawStaticConverterFactory;
import org.infinispan.server.functional.extensions.filters.SimpleConverterFactory;
import org.infinispan.server.functional.extensions.filters.StaticCacheEventFilterFactory;
import org.infinispan.server.functional.extensions.filters.StaticConverterFactory;
import org.infinispan.server.functional.hotrod.HotRodAdmin;
import org.infinispan.server.functional.hotrod.HotRodCacheContinuousQueries;
import org.infinispan.server.functional.hotrod.HotRodCacheEvents;
import org.infinispan.server.functional.hotrod.HotRodCacheOperations;
import org.infinispan.server.functional.hotrod.HotRodCacheQueries;
import org.infinispan.server.functional.hotrod.HotRodCounterOperations;
import org.infinispan.server.functional.hotrod.HotRodListenerWithDslFilter;
import org.infinispan.server.functional.hotrod.HotRodMultiMapOperations;
import org.infinispan.server.functional.hotrod.HotRodTransactionalCacheOperations;
import org.infinispan.server.functional.hotrod.IgnoreCaches;
import org.infinispan.server.functional.memcached.MemcachedOperations;
import org.infinispan.server.functional.rest.RestLoggingResource;
import org.infinispan.server.functional.rest.RestMetricsResource;
import org.infinispan.server.functional.rest.RestOperations;
import org.infinispan.server.functional.rest.RestRouter;
import org.infinispan.server.functional.rest.RestServerResource;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.infinispan.tasks.ServerTask;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Suite
@SelectClasses({
      // This needs to be first as it collects all metrics and all the tests below add a lot which due to inefficiencies
      // in small rye can be very very slow!
      RestMetricsResource.class,
      HotRodCacheOperations.class,
      RestOperations.class,
      RestRouter.class,
      RestServerResource.class,
      MemcachedOperations.class,
      HotRodAdmin.class,
      HotRodCounterOperations.class,
      HotRodMultiMapOperations.class,
      HotRodTransactionalCacheOperations.class,
      HotRodCacheEvents.class,
      HotRodCacheQueries.class,
      HotRodCacheContinuousQueries.class,
      HotRodListenerWithDslFilter.class,
      IgnoreCaches.class,
      RestLoggingResource.class,
      ScriptingTasks.class,
      ServerTasks.class,
      PojoMarshalling.class
})
public class ClusteredIT extends InfinispanSuite {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(2)
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(mavenArtifacts())
               .artifacts(artifacts())
               .property("infinispan.query.lucene.max-boolean-clauses", "1025")
               .build();

   public static String[] mavenArtifacts() {
      return Common.NASHORN_DEPS;
   }

   public static JavaArchive[] artifacts() {
      JavaArchive hello = ShrinkWrap.create(JavaArchive.class, "hello-server-task.jar")
            .addClass(HelloServerTask.class)
            .addAsServiceProvider(ServerTask.class, HelloServerTask.class);

      JavaArchive distHello = ShrinkWrap.create(JavaArchive.class, "distributed-hello-server-task.jar")
            .addPackage(DistributedHelloServerTask.class.getPackage())
            .addAsServiceProvider(ServerTask.class, DistributedHelloServerTask.class);

      JavaArchive isolated = ShrinkWrap.create(JavaArchive.class, "isolated-server-task.jar")
            .addPackage(IsolatedTask.class.getPackage())
            .addAsServiceProvider(ServerTask.class, IsolatedTask.class);

      JavaArchive shared = ShrinkWrap.create(JavaArchive.class, "shared-server-task.jar")
            .addPackage(SharedTask.class.getPackage())
            .addAsServiceProvider(ServerTask.class, SharedTask.class);

      JavaArchive pojo = ShrinkWrap.create(JavaArchive.class, "pojo.jar")
            .addClass(Person.class);

      JavaArchive filterFactories = ShrinkWrap.create(JavaArchive.class, "filter-factories.jar")
            .addPackage(DynamicCacheEventFilterFactory.class.getPackage())
            .addPackage(Entities.class.getPackage())
            .addAsServiceProvider(CacheEventFilterFactory.class,
                  DynamicCacheEventFilterFactory.class,
                  RawStaticCacheEventFilterFactory.class,
                  StaticCacheEventFilterFactory.class)
            .addAsServiceProvider(CacheEventConverterFactory.class,
                  DynamicConverterFactory.class,
                  RawStaticConverterFactory.class,
                  SimpleConverterFactory.class,
                  StaticConverterFactory.class)
            .addAsServiceProvider(CacheEventFilterConverterFactory.class,
                  FilterConverterFactory.class)
            .addAsServiceProvider(SerializationContextInitializer.class, EntitiesImpl.class);

      return new JavaArchive[]{hello, distHello, isolated, shared, pojo, filterFactories};
   }
}
