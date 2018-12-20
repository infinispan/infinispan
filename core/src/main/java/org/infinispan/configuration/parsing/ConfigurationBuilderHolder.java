package org.infinispan.configuration.parsing;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;

public class ConfigurationBuilderHolder {

   private GlobalConfigurationBuilder globalConfigurationBuilder;
   private final Map<String, ConfigurationBuilder> namedConfigurationBuilders;
   private ConfigurationBuilder currentConfigurationBuilder;
   private final Map<Class<? extends ConfigurationParser>, ParserContext> parserContexts;
   private final WeakReference<ClassLoader> classLoader;
   private final Stack<String> scope;
   private final Map<String, JGroupsChannelConfigurator> jgroupsStacks;

   public ConfigurationBuilderHolder() {
      this(Thread.currentThread().getContextClassLoader());
   }

   public ConfigurationBuilderHolder(ClassLoader classLoader) {
      this.globalConfigurationBuilder = new GlobalConfigurationBuilder();
      this.namedConfigurationBuilders = new HashMap<>();
      this.parserContexts = new HashMap<>();
      this.classLoader = new WeakReference<>(classLoader);
      scope = new Stack<>();
      scope.push(ParserScope.GLOBAL.name());
      this.jgroupsStacks = new HashMap<>();
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

   void pushScope(Enum<?> scope) {
      pushScope(scope.name());
   }

   public void pushScope(String scope) {
      this.scope.push(scope);
   }

   public String popScope() {
      return this.scope.pop();
   }

   public boolean inScope(String scope) {
      return getScope().equals(scope);
   }

   public boolean inScope(Enum<?> scope) {
      return inScope(scope.name());
   }

   public String getScope() {
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

   public void addJGroupsStack(FileJGroupsChannelConfigurator stack) {
      String name = stack.getName();
      if (jgroupsStacks.containsKey(name)) {
         throw Parser.log.duplicateJGroupsStack(name);
      }
      jgroupsStacks.put(name, stack);
   }

   public void addJGroupsStack(EmbeddedJGroupsChannelConfigurator stack, String extend) {
      String name = stack.getName();
      if (jgroupsStacks.containsKey(name)) {
         throw Parser.log.duplicateJGroupsStack(name);
      }

      if (extend == null) {
         // Add as is
         jgroupsStacks.put(stack.getName(), stack);
      } else {
         // See if the parent exists
         if (!jgroupsStacks.containsKey(extend)) {
            throw Parser.log.missingJGroupsStack(extend);
         } else {
            JGroupsChannelConfigurator baseStack = jgroupsStacks.get(extend);
            jgroupsStacks.put(stack.getName(), EmbeddedJGroupsChannelConfigurator.combine(baseStack, stack));
         }
      }
   }

   public JGroupsChannelConfigurator getJGroupsStack(String name) {
      return jgroupsStacks.get(name);
   }

}
