package org.infinispan.server.integration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;

import org.jboss.arquillian.container.spi.Container;
import org.jboss.arquillian.container.spi.ContainerRegistry;
import org.jboss.arquillian.core.api.annotation.Observes;
import org.jboss.arquillian.core.spi.ServiceLoader;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;

/**
 * Responsible for packaging the infinispan-client-hotrod from source code and update the module.xml
 */
public class WildflyResoucesProcessorExecuter {

   public static final String HOTROD_MODULE_NAME = "org.infinispan.client.hotrod";

   public void registerInstance(@Observes ContainerRegistry registry, ServiceLoader serviceLoader) throws IOException {

      if (isWildfly(registry.getContainers())) {

         /*
          * If you are debugging, the file will be: /tmp/infinispan-client-hotrod-15309020926398475441.jar
          * If you are running from the command line, the file will be: infinispan-client-hotrod-11.0.0-SNAPSHOT.jar
          */
         File[] libs = Maven.resolver()
               .loadPomFromFile("pom.xml").resolve("org.infinispan:infinispan-client-hotrod")
               .withoutTransitivity().as(File.class);

         File moduleHome = new File(System.getProperty("jbossHome"), "modules/system/layers/base/org/infinispan/client/hotrod/main");
         // we clean to avoid an kind of issue with jar files in the module folder
         for (String file : moduleHome.list()) {
            if (file.endsWith(".jar")) {
               new File(moduleHome, file).delete();
            }
         }
         // we add back infinispan-client-hotrod
         for (File f : libs) {
            File target = new File(moduleHome, f.getName());
            Files.copy(f.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
         }

         WildflyModuleOverwrite.change(new File(moduleHome, "module.xml"), Arrays.asList(libs).stream().filter(f -> f.getName().startsWith("infinispan-client-hotrod")).findFirst().get().getName());
      }
   }

   private boolean isWildfly(List<Container> containers) {
      boolean isWildfly = false;
      for (Container c : containers) {
         if (c.getName().startsWith("wildfly")) {
            isWildfly = true;
            break;
         }
      }
      return isWildfly;
   }
}
