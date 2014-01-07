package org.infinispan.configuration.parsing;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

public class ConfigurationBuilderHolder {

   private final GlobalConfigurationBuilder globalConfigurationBuilder;
   private final ConfigurationBuilder defaultConfigurationBuilder;
   private final Map<String, ConfigurationBuilder> namedConfigurationBuilders;
   private ConfigurationBuilder currentConfigurationBuilder;
   private final Map<Class<? extends ConfigurationParser>, ParserContext> parserContexts;
   private final WeakReference<ClassLoader> classLoader;
   private String defaultCacheName;

   public ConfigurationBuilderHolder() {
      this(Thread.currentThread().getContextClassLoader());
   }

   public ConfigurationBuilderHolder(ClassLoader classLoader) {
      this.globalConfigurationBuilder = new GlobalConfigurationBuilder();
      this.defaultConfigurationBuilder = new ConfigurationBuilder();
      this.namedConfigurationBuilders = new HashMap<String, ConfigurationBuilder>();
      this.currentConfigurationBuilder = defaultConfigurationBuilder;
      this.parserContexts = new HashMap<Class<? extends ConfigurationParser>, ParserContext>();
      this.classLoader = new WeakReference<ClassLoader>(classLoader);
   }

   public GlobalConfigurationBuilder getGlobalConfigurationBuilder() {
      return globalConfigurationBuilder;
   }

   public ConfigurationBuilder newConfigurationBuilder(String name) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      //no need to validate default config again
      //https://issues.jboss.org/browse/ISPN-1938
      builder.read(getDefaultConfigurationBuilder().build(false));
      namedConfigurationBuilders.put(name, builder);
      currentConfigurationBuilder = builder;
      return builder;
   }

   public ConfigurationBuilder getDefaultConfigurationBuilder() {
      ConfigurationBuilder builder = namedConfigurationBuilders.get(defaultCacheName);
      return builder == null ? defaultConfigurationBuilder : builder;
   }

   public Map<String, ConfigurationBuilder> getNamedConfigurationBuilders() {
      return namedConfigurationBuilders;
   }

   public ConfigurationBuilder getCurrentConfigurationBuilder() {
      return currentConfigurationBuilder;
   }

   @SuppressWarnings("unchecked")
   public <T extends ParserContext> T getParserContext(Class<? extends ConfigurationParser> parserClass) {
      return (T) parserContexts.get(parserClass);
   }

   public void setParserContext(Class<? extends ConfigurationParser> parserClass, ParserContext context) {
      parserContexts.put(parserClass, context);
   }

   public ClassLoader getClassLoader() {
      return classLoader.get();
   }

   Map<Class<? extends ConfigurationParser>, ParserContext> getParserContexts() {
      return parserContexts;
   }

   public void setDefaultCacheName(String defaultCacheName) {
      this.defaultCacheName = defaultCacheName;
   }
}
