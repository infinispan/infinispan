package org.infinispan.test.integration.thirdparty;

import java.io.File;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.FileAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.MavenResolverSystem;

public class DeploymentHelper {

   private static final MavenResolverSystem MAVEN_RESOLVER = Maven.configureResolver().fromFile("../../../maven-settings.xml");
   private static final String ARQUILLIAN_LAUNCH = System.getProperty("arquillian.launch");

   public static WebArchive createDeployment() {

      WebArchive war = ShrinkWrap.create(WebArchive.class, "infinispan-server-integration.war");

      if (isTomcat()) {
         tomcat(war);
      } else if (isWildfly()) {
         wildfly(war);
      } else {
         throw new IllegalStateException(String.format("'%s' not supported", ARQUILLIAN_LAUNCH));
      }

      return war;
   }

   public static boolean isTomcat() {
      return "tomcat".equals(ARQUILLIAN_LAUNCH);
   }

   public static boolean isWildfly() {
      return "wildfly".equals(ARQUILLIAN_LAUNCH);
   }

   public static void addLibrary(WebArchive war, String canonicalForm) {
      addLibrary(war, MAVEN_RESOLVER
            .loadPomFromFile("pom.xml")
            .resolve(canonicalForm)
            .withTransitivity().as(File.class));
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
