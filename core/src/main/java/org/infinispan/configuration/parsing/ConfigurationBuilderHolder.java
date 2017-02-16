package org.infinispan.configuration.parsing;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

public class ConfigurationBuilderHolder {

   private final GlobalConfigurationBuilder globalConfigurationBuilder;
   private final Map<String, ConfigurationBuilder> namedConfigurationBuilders;
   private ConfigurationBuilder currentConfigurationBuilder;
   private final Map<Class<? extends ConfigurationParser>, ParserContext> parserContexts;
   private final WeakReference<ClassLoader> classLoader;
   private final Stack<ParserScope> scope;

   public ConfigurationBuilderHolder() {
      this(Thread.currentThread().getContextClassLoader());
   }

   public ConfigurationBuilderHolder(ClassLoader classLoader) {
      this.globalConfigurationBuilder = new GlobalConfigurationBuilder();
      this.namedConfigurationBuilders = new HashMap<>();
      this.parserContexts = new HashMap<>();
      this.classLoader = new WeakReference<>(classLoader);
      scope = new Stack<>();
      scope.push(ParserScope.GLOBAL);
   }

   public GlobalConfigurationBuilder getGlobalConfigurationBuilder() {
      return globalConfigurationBuilder;
   }

   public ConfigurationBuilder newConfigurationBuilder(String name) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      namedConfigurationBuilders.put(name, builder);
      currentConfigurationBuilder = builder;
      return builder;
   }

   public Map<String, ConfigurationBuilder> getNamedConfigurationBuilders() {
      return namedConfigurationBuilders;
   }

   public ConfigurationBuilder getCurrentConfigurationBuilder() {
      return currentConfigurationBuilder;
   }

   public ConfigurationBuilder getDefaultConfigurationBuilder() {
      if (globalConfigurationBuilder.defaultCacheName().isPresent()) {
         return namedConfigurationBuilders.get(globalConfigurationBuilder.defaultCacheName().get());
      } else {
         return null;
      }
   }

   void pushScope(ParserScope scope) {
      this.scope.push(scope);
   }

   void popScope() {
      this.scope.pop();
   }

   public ParserScope getScope() {
      return scope.peek();
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

   public void validate() {
      globalConfigurationBuilder.defaultCacheName().ifPresent(name -> {
         if (!namedConfigurationBuilders.containsKey(name))
            throw Parser.log.missingDefaultCacheDeclaration(name);
      });
   }
}
