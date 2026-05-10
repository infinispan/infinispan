package org.infinispan.test.integration.thirdparty;

import java.io.File;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.FileAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.MavenResolverSystem;

public class DeploymentHelper {

   private static final String ARQUILLIAN_LAUNCH = System.getProperty("arquillian.launch");

   public static WebArchive createDeployment() {
      WebArchive war = ShrinkWrap.create(WebArchive.class, "infinispan-server-integration.war");
      addLibrary(war, "org.junit.jupiter:junit-jupiter-api");
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

   private static void wildfly(WebArchive war) {
      File file = new File(DeploymentHelper.class.getClassLoader().getResource("wildfly/jboss-deployment-structure.xml").getFile());
      war.add(new FileAsset(file), "WEB-INF/jboss-deployment-structure.xml");
   }

   private static void tomcat(WebArchive war) {
      // required for cdi
      addLibrary(war, "org.jboss.weld.servlet:weld-servlet-core");
   }

   private static final MavenResolverSystem MAVEN_RESOLVER = Maven.resolver();

   public static void addLibrary(WebArchive war, String canonicalForm) {
      addLibrary(war, MAVEN_RESOLVER
            .loadPomFromFile("pom.xml")
            .resolve(canonicalForm)
            .withTransitivity().as(File.class));
   }

   private static void addLibrary(WebArchive war, File[]... libGroup) {
      for (File[] group : libGroup) {
         war.addAsLibraries(group);
      }
   }
}
