package org.infinispan.hibernate.search.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.hibernate.search.engine.service.classloading.spi.ClassLoaderService;
import org.hibernate.search.engine.service.spi.ServiceManager;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;

/**
 * The Infinispan configuration is ClassLoader sensitive, this wrapper around the standard Parser is used to allow it to
 * find resources in a modular classloading environment.
 *
 * @author Sanne Grinovero
 * @since 4.3
 */
public class InfinispanConfigurationParser {

   private final ParserRegistry configurationParser;
   private final ClassLoader ispnClassLoadr;

   public InfinispanConfigurationParser() {
      //This class instance will have visibility on the components we need
      ispnClassLoadr = InfinispanConfigurationParser.class.getClassLoader();
      configurationParser = new ParserRegistry(ispnClassLoadr);
   }

   /**
    * Resolves an Infinispan configuration file but using the Hibernate Search classloader. The returned Infinispan
    * configuration template also overrides Infinispan's runtime classloader to the one of Hibernate Search.
    *
    * @param filename                  Infinispan configuration resource name
    * @param transportOverrideResource An alternative JGroups configuration file to be injected
    * @param serviceManager            the ServiceManager to load resources
    * @return
    * @throws IOException
    */
   public ConfigurationBuilderHolder parseFile(String filename, String transportOverrideResource, ServiceManager serviceManager) throws IOException {
      ClassLoaderService classLoaderService = serviceManager.requestService(ClassLoaderService.class);
      try {
         return parseFile(classLoaderService, filename, transportOverrideResource);
      } finally {
         serviceManager.releaseService(ClassLoaderService.class);
      }
   }

   private ConfigurationBuilderHolder parseFile(ClassLoaderService classLoaderService, String filename, String transportOverrideResource) throws IOException {
      URL url = classLoaderService.locateResource(filename);

      //Infinispan requires the context ClassLoader to have full visibility on all
      //its components and eventual extension points even *during* configuration parsing.
      final Thread currentThread = Thread.currentThread();
      final ClassLoader originalContextClassLoader = currentThread.getContextClassLoader();
      try {
         currentThread.setContextClassLoader(ispnClassLoadr);
         ConfigurationBuilderHolder builderHolder = configurationParser.parse(url);
         //Workaround Infinispan's ClassLoader strategies to bend to our will:
         fixClassLoaders(builderHolder);
         patchTransportConfiguration(builderHolder, transportOverrideResource);
         return builderHolder;
      } finally {
         currentThread.setContextClassLoader(originalContextClassLoader);
      }
   }

   private void fixClassLoaders(ConfigurationBuilderHolder builderHolder) {
      //Global section:
      builderHolder.getGlobalConfigurationBuilder().classLoader(ispnClassLoadr);
   }

   /**
    * After having parsed the Infinispan configuration file, we might want to override the specified JGroups
    * configuration file.
    *
    * @param builderHolder
    * @param transportOverrideResource The alternative JGroups configuration file to be used, or null
    */
   private void patchTransportConfiguration(ConfigurationBuilderHolder builderHolder, String transportOverrideResource) throws IOException {
      if (transportOverrideResource != null) {
         try (InputStream xml = FileLookupFactory.newInstance().lookupFileStrict(transportOverrideResource, ispnClassLoadr)) {
            Properties p = new Properties();
            FileJGroupsChannelConfigurator configurator = new FileJGroupsChannelConfigurator("override", transportOverrideResource, xml, System.getProperties());
            p.put(JGroupsTransport.CHANNEL_CONFIGURATOR, configurator);
            builderHolder.getGlobalConfigurationBuilder().transport().withProperties(p);
         }
      }
   }

}
