package org.infinispan.test.integration;

import java.io.File;

import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.MavenResolverSystem;

public class GenericDeploymentHelper {

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
