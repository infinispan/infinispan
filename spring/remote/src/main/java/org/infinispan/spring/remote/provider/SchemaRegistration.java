package org.infinispan.spring.remote.provider;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.protostream.GeneratedSchema;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

/**
 * Discovers {@link GeneratedSchema} implementations on the classpath and registers them
 * with the Infinispan server. This provides a transparent schema registration experience
 * similar to the Quarkus Infinispan client extension.
 *
 * @since 16.2
 */
public final class SchemaRegistration {

   private static final Logger logger = Logger.getLogger(MethodHandles.lookup().lookupClass().getName());

   private SchemaRegistration() {
   }

   /**
    * Scans the given base packages for {@link GeneratedSchema} implementations,
    * excluding internal Infinispan packages.
    *
    * @param classLoader the class loader to use for loading discovered classes
    * @param basePackages the base packages to scan
    * @return a list of discovered {@link GeneratedSchema} instances
    */
   public static List<GeneratedSchema> discoverSchemas(ClassLoader classLoader, List<String> basePackages) {
      ClassPathScanningCandidateComponentProvider scanner =
            new ClassPathScanningCandidateComponentProvider(false);
      scanner.addIncludeFilter(new AssignableTypeFilter(GeneratedSchema.class));

      Set<String> seen = new LinkedHashSet<>();
      List<GeneratedSchema> schemas = new ArrayList<>();

      for (String basePackage : basePackages) {
         for (BeanDefinition bd : scanner.findCandidateComponents(basePackage)) {
            String className = bd.getBeanClassName();
            if (className == null || className.startsWith("org.infinispan.")) {
               continue;
            }
            if (!seen.add(className)) {
               continue;
            }
            try {
               Class<?> clazz = classLoader.loadClass(className);
               GeneratedSchema schema = (GeneratedSchema) clazz.getDeclaredConstructor().newInstance();
               schemas.add(schema);
               logger.log(Level.FINE, "Discovered GeneratedSchema: {0}", className);
            } catch (Exception e) {
               logger.log(Level.WARNING, "Failed to instantiate GeneratedSchema: " + className, e);
            }
         }
      }

      if (!schemas.isEmpty()) {
         logger.log(Level.INFO, "Discovered {0} protobuf schema(s) for auto-registration", schemas.size());
      }

      return schemas;
   }

   /**
    * Uploads the given schemas to the Infinispan server using the {@link RemoteSchemasAdmin} API.
    *
    * @param cacheManager the remote cache manager (must be started)
    * @param schemas the schemas to upload
    */
   public static void uploadSchemas(RemoteCacheManager cacheManager, Collection<GeneratedSchema> schemas) {
      if (schemas.isEmpty()) {
         return;
      }

      try {
         RemoteSchemasAdmin schemasAdmin = cacheManager.administration().schemas();
         for (GeneratedSchema schema : schemas) {
            try {
               RemoteSchemasAdmin.SchemaOpResult result = schemasAdmin.createOrUpdate(schema);
               if (result.hasError()) {
                  logger.log(Level.WARNING, "Schema ''{0}'' registered with validation error: {1}",
                        new Object[]{schema.getName(), result.getError()});
               } else {
                  logger.log(Level.INFO, "Registered schema ''{0}'' on the server", schema.getName());
               }
            } catch (Exception e) {
               logger.log(Level.WARNING, "Failed to upload schema '" + schema.getName() + "' to server", e);
            }
         }
      } catch (Exception e) {
         logger.log(Level.WARNING, "Failed to access schema administration", e);
      }
   }
}
