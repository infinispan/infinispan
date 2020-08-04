package org.infinispan.server.core.backup.resources;

import static org.infinispan.server.core.BackupManager.Resources.Type.CACHE_CONFIGURATIONS;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.zip.ZipFile;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.BackupManager;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * {@link org.infinispan.server.core.backup.ContainerResource} implementation for {@link
 * BackupManager.Resources.Type#CACHE_CONFIGURATIONS}.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
class CacheConfigResource extends AbstractContainerResource {

   private final ParserRegistry parserRegistry;
   private final EmbeddedCacheManager cm;

   CacheConfigResource(BlockingManager blockingManager, ParserRegistry parserRegistry, EmbeddedCacheManager cm,
                       BackupManager.Resources params, Path root) {
      super(CACHE_CONFIGURATIONS, params, blockingManager, root);
      this.cm = cm;
      this.parserRegistry = parserRegistry;
   }

   @Override
   public void prepareAndValidateBackup() {
      Set<String> configs = wildcard ? cm.getCacheConfigurationNames() : resources;
      for (String configName : configs) {
         Configuration config = cm.getCacheConfiguration(configName);

         if (wildcard) {
            // For wildcard resources, we ignore internal caches, however explicitly requested internal caches are allowed
            if (!config.isTemplate()) {
               continue;
            }
            resources.add(configName);
         } else if (config == null) {
            throw log.unableToFindResource(type.toString(), configName);
         } else if (!config.isTemplate()) {
            throw new CacheException(String.format("Unable to backup %s '%s' as it is not a template", type, configName));
         }
      }
   }

   @Override
   public CompletionStage<Void> backup() {
      return blockingManager.runBlocking(() -> {
         mkdirs(root);

         for (String configName : resources) {
            Configuration config = cm.getCacheConfiguration(configName);
            String fileName = configFile(configName);
            Path xmlPath = root.resolve(String.format("%s.xml", configName));
            try (OutputStream os = Files.newOutputStream(xmlPath)) {
               parserRegistry.serialize(os, configName, config);
            } catch (XMLStreamException | IOException e) {
               throw new CacheException(String.format("Unable to create backup file '%s'", fileName), e);
            }
         }
      }, "cache-config-write");
   }

   @Override
   public CompletionStage<Void> restore(ZipFile zip) {
      return blockingManager.runBlocking(() -> {
         for (String configName : resources) {
            String configFile = configFile(configName);
            String zipPath = root.resolve(configFile).toString();
            try (InputStream is = zip.getInputStream(zip.getEntry(zipPath))) {
               ConfigurationBuilderHolder builderHolder = parserRegistry.parse(is, null);
               ConfigurationBuilder builder = builderHolder.getNamedConfigurationBuilders().get(configName);
               Configuration cfg = builder.template(true).build();

               // Only define configurations that don't already exist so that we don't overwrite newer versions of the default
               // templates e.g. org.infinispan.DIST_SYNC when upgrading a cluster
               if (cm.getCacheConfiguration(configName) == null)
                  cm.defineConfiguration(configName, cfg);
            } catch (IOException e) {
               throw new CacheException(e);
            }
         }
      }, "cache-config-read");
   }

   private String configFile(String config) {
      return String.format("%s.xml", config);
   }
}
