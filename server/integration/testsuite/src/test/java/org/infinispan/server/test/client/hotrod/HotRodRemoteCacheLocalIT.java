package org.infinispan.server.test.client.hotrod;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.commons.test.categories.Smoke;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({Smoke.class})
@WithRunningServer(@RunningServer(name = "filters-local"))
public class HotRodRemoteCacheLocalIT extends AbstractRemoteCacheIT {

   @InfinispanResource("filters-local")
   private RemoteInfinispanServer server1;

   private static final Set<File> deployments = new HashSet<>();

   @BeforeClass
   public static void before() {
      Archive<?> pojoArchive = createPojoArchive();
      Archive<?> filterArchive = createFilterArchive();
      Archive<?> converterArchive = createConverterArchive();
      Archive<?> filterConverterArchive = createFilterConverterArchive();
      Archive<?> keyValueFilterConverterArchive = createKeyValueFilterConverterArchive();

      deployToServers(pojoArchive, "pojo.jar");
      deployToServers(filterArchive, "filter.jar");
      deployToServers(converterArchive, "converter.jar");
      deployToServers(filterConverterArchive, "filter-converter.jar");
      deployToServers(keyValueFilterConverterArchive, "key-value-filter-converter.jar");
   }

   private static void deployToServers(Archive<?> archive, String jarName) {
      File deployment = new File(System.getProperty("server1.dist"), "/standalone/deployments/" + jarName);
      archive.as(ZipExporter.class).exportTo(deployment, true);
      deployments.add(deployment);
   }

   @AfterClass
   public static void after() {
      release();
      deployments.forEach(File::delete);
   }

   @Override
   protected List<RemoteInfinispanServer> getServers() {
      List<RemoteInfinispanServer> servers = new ArrayList<>();
      servers.add(server1);
      return Collections.unmodifiableList(servers);
   }

   @Override
   protected String getCacheManagerName() {
      return "local";
   }
}
