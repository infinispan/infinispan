package org.infinispan.server.integration;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.jboss.shrinkwrap.api.Filters;
import org.jboss.shrinkwrap.api.GenericArchive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.importer.ExplodedImporter;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.MavenFormatStage;
import org.jboss.shrinkwrap.resolver.api.maven.MavenResolverSystemBase;
import org.jboss.shrinkwrap.resolver.api.maven.MavenStrategyStage;
import org.jboss.shrinkwrap.resolver.api.maven.PomEquippedResolveStage;
import org.jboss.shrinkwrap.resolver.api.maven.PomlessResolveStage;

/**
 * Responsible to build the war when needed
 */
public class DeploymentBuilder {

   public static WebArchive war() {
      WebArchive war = ShrinkWrap.create(WebArchive.class, "infinispan-server-integration.war");
      addDefaultTestLib(war);
      ArquillianServerType arquillianServerType = ArquillianServerType.current();
      if (ArquillianServerType.TOMCAT9.equals(arquillianServerType)) {
         tomcat9(war);
      } else if (ArquillianServerType.WILDFLY18.equals(arquillianServerType)) {
         wildfly18(war);
      } else {
         throw new IllegalStateException(String.format("%s not supported", arquillianServerType));
      }
      for (File file : new File("src/main/resources").listFiles()) {
         war.addAsResource(file, file.getName());
      }
      return war;
   }

   private static void addDefaultTestLib(WebArchive war) {
      File[] testdriverJunit4 = newMavenConfigureResolver()
            .loadPomFromFile("pom.xml")
            .resolve("org.infinispan:infinispan-server-testdriver-junit4")
            .withTransitivity().as(File.class);
      File[] testdriverCore = newMavenConfigureResolver()
            .loadPomFromFile("pom.xml")
            .resolve("org.infinispan:infinispan-server-testdriver-core")
            .withTransitivity().as(File.class);
      addLibrary(war, testdriverJunit4, testdriverCore);
   }

   private static WebArchive tomcat9(WebArchive war) {
      String tomcat9RootFolder = "src/server-config/tomcat9/webapp";
      war.merge(ShrinkWrap.create(GenericArchive.class).as(ExplodedImporter.class).importDirectory(tomcat9RootFolder)
            .as(GenericArchive.class), "/", Filters.includeAll());
      war.setWebXML(new File(tomcat9RootFolder, "WEB-INF/web.xml"));
      File[] clientHotRod = newMavenConfigureResolver()
            .loadPomFromFile("pom.xml")
            .resolve("org.infinispan:infinispan-client-hotrod")
            .withTransitivity().as(File.class);
      File[] marshalling = newMavenConfigureResolver()
            .loadPomFromFile("pom.xml")
            .resolve("org.infinispan:infinispan-jboss-marshalling")
            .withTransitivity().as(File.class);
      addLibrary(war, clientHotRod, marshalling);
      return war;
   }

   private static WebArchive wildfly18(WebArchive war) {
      String jbossDeploymentStructure = "<jboss-deployment-structure xmlns=\"urn:jboss:deployment-structure:1.1\">\n" +
            "    <deployment>\n" +
            "        <dependencies>\n" +
            "            <system export=\"true\">\n" +
            "                <paths>\n" +
            "                    <path name=\"sun/reflect\"/>\n" +
            "                </paths>\n" +
            "            </system>\n" +
            "        </dependencies>\n" +
            "    </deployment>\n" +
            "</jboss-deployment-structure>";
      war.add(new StringAsset(jbossDeploymentStructure), "WEB-INF/jboss-deployment-structure.xml");
      addLibrary(war, newMavenConfigureResolver()
            .loadPomFromFile("pom.xml")
            .resolve("org.infinispan:infinispan-client-hotrod")
            .withTransitivity().as(File.class));
      addLibrary(war, newMavenConfigureResolver()
            .loadPomFromFile("pom.xml")
            .resolve("org.infinispan:infinispan-jboss-marshalling")
            .withTransitivity().as(File.class));
      return war;
   }

   private static MavenResolverSystemBase<PomEquippedResolveStage, PomlessResolveStage, MavenStrategyStage, MavenFormatStage> newMavenConfigureResolver() {
      Path setting = Paths.get(System.getProperty("jboss.modules.settings.xml.url")).normalize();
      String settingPath = setting.toString().replace("file:", "");
      return Maven.configureResolver().fromFile(settingPath);
   }

   private static void addLibrary(WebArchive war, File[]... libGroup) {
      for (File[] group : libGroup) {
         war.addAsLibraries(group);
      }
   }
}
