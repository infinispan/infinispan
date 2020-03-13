package org.infinispan.commons.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BlueprintGenerator {

   public static void main(String[] args) {
      if (args.length != 1) {
         throw new RuntimeException("The build directory argument is required!");
      }
      final String servicesSrcDir = args[0] + "/META-INF/services";
      final String blueprintOutputFile = args[0] + "/OSGI-INF/blueprint/blueprint.xml";

      try {
         writeBeanDefinitions(createBeanDefinitions(servicesSrcDir), blueprintOutputFile);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static void writeBeanDefinitions(String beanDefinitions, String blueprintOutputFile) throws IOException {
      String template = new String(Files.readAllBytes(Paths.get(blueprintOutputFile)));
      Files.write(Paths.get(blueprintOutputFile), template.replace("${services}", beanDefinitions).getBytes());
   }

   private static String createBeanDefinitions(String servicesDir) throws IOException {
      StringBuilder beanDefinitions = new StringBuilder();
      try (Stream<Path> services = Files.list(Paths.get(servicesDir))) {
         services.forEach(service -> {
            try (Stream<String> serviceImpls = Files.lines(service)) {
               String bean = serviceImpls.map(serviceImpl -> beanDefinition(service.getFileName().toString(), serviceImpl))
                       .collect(Collectors.joining("\n"));
               beanDefinitions.append(bean);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
         });
      }
      return beanDefinitions.toString();
   }

   private static String beanDefinition(String serviceName, String serviceImpl) {
      String id = serviceImpl.substring(serviceImpl.lastIndexOf(".")+1).toLowerCase();
      StringBuilder bld = new StringBuilder();
      bld.append(String.format("<bean id=\"%s\" class=\"%s\" />\n", id, serviceImpl));
      bld.append(String.format("<service ref=\"%s\" interface=\"%s\" />\n", id, serviceName));
      return bld.toString();
   }

}
