package org.infinispan.server.integration;

import java.io.File;

import org.jboss.shrinkwrap.api.Filters;
import org.jboss.shrinkwrap.api.GenericArchive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.importer.ExplodedImporter;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.PomEquippedResolveStage;

/**
 * Responsible to build the war
 */
public class DeploymentBuilder {

   enum ArquillianServer {
      TOMCAT9, WILDFLY18
   }

   public static WebArchive war() {

      ArquillianServer server = ArquillianServer.valueOf(System.getProperty("infinispan.server.integration.launch").toUpperCase());
      WebArchive war = ShrinkWrap.create(WebArchive.class, "infinispan-server-integration.war");
      if (ArquillianServer.TOMCAT9.equals(server)) {
         tomcat9(war);
      } else if (ArquillianServer.WILDFLY18.equals(server)) {
         wildfly18(war);
      } else {
         throw new IllegalStateException(String.format("%s not supported", server));
      }

      for (File file : new File("src/main/resources").listFiles()) {
         war.addAsResource(file, file.getName());
      }
      return war;
   }

   private static WebArchive tomcat9(WebArchive war) {
      String tomcat9RootFolder = "src/server-config/tomcat9/webapp";
      war.merge(ShrinkWrap.create(GenericArchive.class).as(ExplodedImporter.class).importDirectory(tomcat9RootFolder)
            .as(GenericArchive.class), "/", Filters.includeAll());
      war.setWebXML(new File(tomcat9RootFolder, "WEB-INF/web.xml"));
      File[] libs = Maven.resolver()
            .loadPomFromFile("pom.xml").resolve("org.infinispan:infinispan-client-hotrod")
            .withoutTransitivity().as(File.class);
      for (File file : libs) {
         war.addAsLibrary(file);
      }
      return war;
   }

   private static WebArchive wildfly18(WebArchive war) {
      String manifest = Descriptors.create(ManifestDescriptor.class).attribute("Dependencies", WildflyResoucesProcessorExecuter.HOTROD_MODULE_NAME).exportAsString();
      war.add(new StringAsset(manifest), "META-INF/MANIFEST.MF");
      return war;
   }
}
