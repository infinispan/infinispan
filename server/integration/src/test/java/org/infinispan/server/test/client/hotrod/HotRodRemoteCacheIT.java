package org.infinispan.server.test.client.hotrod;

import java.io.File;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Filters;
import org.jboss.shrinkwrap.api.GenericArchive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.importer.ExplodedImporter;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.PomEquippedResolveStage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for the HotRod client RemoteCache class deployed on another server
 */
@RunWith(Arquillian.class)
public class HotRodRemoteCacheIT {

   private RemoteCacheManager remoteCacheManager;
   private RemoteCache remoteCache;

   @Before
   public void before() {
      remoteCacheManager = new RemoteCacheManager(new ConfigurationBuilder().addServer().host("127.0.0.1").port(11222).build());
      remoteCache =  remoteCacheManager.getCache("integration.REPL_SYNC");
   }

   @After
   public void after() {
      remoteCacheManager.stop();
   }

   @Test
   public void testReplaceWithVersionWithLifespan() throws Exception {
      remoteCache.put("foo", "bar");
      Assert.assertEquals("bar", remoteCache.get("foo"));
   }

   // TODO create WebArchive based on server on classpath
   // TODO the webapp must be called tomcat9 and/or wildfly
   @Deployment
   public static WebArchive createDeployment() {
      PomEquippedResolveStage mavenResolver = Maven.resolver().loadPomFromFile("pom.xml");
      File[] libs = mavenResolver.importRuntimeAndTestDependencies().resolve().withTransitivity().asFile();

      WebArchive war = ShrinkWrap.create(WebArchive.class, "hotrod-tomcat-example.war");
      war.merge(ShrinkWrap.create(GenericArchive.class).as(ExplodedImporter.class).importDirectory("src/main/webapp")
            .as(GenericArchive.class), "/", Filters.includeAll());

      for (File file : libs) {
         // we don't need deploy those artifacts
         if (file.getName().contains("arquillian") || file.getName().contains("infinispan-server-runtime") || file.getName().contains("shrinkwrap") || file.getName().contains("maven")) {
            continue;
         }
         war.addAsLibrary(file);
      }
      for (File file : new File("src/main/resources").listFiles()) {
         war.addAsResource(file, file.getName());
      }
      war.setWebXML(new File("src/main/webapp", "WEB-INF/web.xml"));
      System.out.println(war.toString(true));
      return war;
   }
}
