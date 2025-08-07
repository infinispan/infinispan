package org.infinispan.server.test.artifacts;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.server.functional.extensions.DistributedHelloServerTask;
import org.infinispan.server.functional.extensions.HelloServerTask;
import org.infinispan.server.functional.extensions.IsolatedTask;
import org.infinispan.server.functional.extensions.Person;
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
import org.infinispan.tasks.ServerTask;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

public class Artifacts {
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

   public static void main(String... args) throws IOException {
      JavaArchive[] artifacts = artifacts();
      Path target = Paths.get(args[0]);
      Files.createDirectories(target);
      for (JavaArchive artifact : artifacts) {
         File jar = target.resolve(artifact.getName()).toFile();
         jar.setWritable(true, false);
         artifact.as(ZipExporter.class).exportTo(jar, true);
      }
   }
}
