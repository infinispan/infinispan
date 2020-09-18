package org.infinispan.test.integration;

import java.io.File;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.FileAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.MavenResolverSystem;

public class DeploymentHelper {

   public static WebArchive createDeployment() {

      WebArchive war = ShrinkWrap.create(WebArchive.class, "infinispan-server-integration.war");
      String arquillianLaunch = System.getProperty("arquillian.launch");

      if (arquillianLaunch.equals("tomcat")) {
         tomcat(war);
      } else if (arquillianLaunch.equals("wildfly") || arquillianLaunch.equals("eap")) {
         wildfly(war);
      } else {
         throw new IllegalStateException(String.format("%s not supported", arquillianLaunch));
      }

      MavenResolverSystem resolver = Maven.resolver();
      addLibrary(war, resolver
            .loadPomFromFile("pom.xml")
            .resolve("org.infinispan:infinispan-client-hotrod")
            .withTransitivity().as(File.class));
      addLibrary(war, resolver
            .loadPomFromFile("pom.xml")
            .resolve("net.spy:spymemcached")
            .withTransitivity().as(File.class));
      return war;
   }

   private static void wildfly(WebArchive war) {
      war.add(new FileAsset(new File("src/test/resources/wildfly/jboss-deployment-structure.xml")), "WEB-INF/jboss-deployment-structure.xml");
   }

   private static void tomcat(WebArchive war) {
      // intentionally empty. if you need to implement a custom deploy the code is 'ready'
   }

   private static void addLibrary(WebArchive war, File[]... libGroup) {
      for (File[] group : libGroup) {
         war.addAsLibraries(group);
      }
   }
}
