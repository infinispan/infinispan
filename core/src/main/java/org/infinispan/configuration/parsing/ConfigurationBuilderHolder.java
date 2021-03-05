package org.infinispan.configuration.parsing;

import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationReaderContext;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.JGroupsConfigurationBuilder;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;

public class ConfigurationBuilderHolder implements ConfigurationReaderContext {

   private final GlobalConfigurationBuilder globalConfigurationBuilder;
   private final Map<String, ConfigurationBuilder> namedConfigurationBuilders;
   private ConfigurationBuilder currentConfigurationBuilder;
   private final Map<Class<? extends ConfigurationParser>, ParserContext> parserContexts;
   private final WeakReference<ClassLoader> classLoader;
   private final Deque<String> scope;
   private final JGroupsConfigurationBuilder jgroupsBuilder;
   private NamespaceMappingParser namespaceMappingParser;

   public ConfigurationBuilderHolder() {
      this(Thread.currentThread().getContextClassLoader());
   }

   public ConfigurationBuilderHolder(ClassLoader classLoader) {
      this(classLoader, new GlobalConfigurationBuilder().classLoader(classLoader));
   }

   public ConfigurationBuilderHolder(ClassLoader classLoader, GlobalConfigurationBuilder globalConfigurationBuilder) {
      this.globalConfigurationBuilder = globalConfigurationBuilder;
      this.namedConfigurationBuilders = new HashMap<>();
      this.parserContexts = new HashMap<>();
      this.jgroupsBuilder = this.globalConfigurationBuilder.transport().jgroups();
      this.classLoader = new WeakReference<>(classLoader);
      scope = new ArrayDeque<>();
      scope.push(ParserScope.GLOBAL.name());
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
            throw CONFIG.missingDefaultCacheDeclaration(name);
      });
   }

   public void addJGroupsStack(FileJGroupsChannelConfigurator stack) {
      String name = stack.getName();
      if (jgroupsBuilder.getStack(name) != null) {
         throw CONFIG.duplicateJGroupsStack(name);
      }
      jgroupsBuilder.addStackFile(name).fileChannelConfigurator(stack);
   }

   public void addJGroupsStack(EmbeddedJGroupsChannelConfigurator stack, String extend) {
      String name = stack.getName();
      if (jgroupsBuilder.getStack(name) != null) {
         throw CONFIG.duplicateJGroupsStack(name);
      }

      if (extend == null) {
         // Add as is
         jgroupsBuilder.addStack(stack.getName()).channelConfigurator(stack);
      } else {
         // See if the parent exists
         if (jgroupsBuilder.getStack(extend) == null) {
            throw CONFIG.missingJGroupsStack(extend);
         } else {
            JGroupsChannelConfigurator baseStack = jgroupsBuilder.getStack(extend);
            jgroupsBuilder.addStack(stack.getName()).channelConfigurator(EmbeddedJGroupsChannelConfigurator.combine(baseStack, stack));
         }
      }
   }

   JGroupsChannelConfigurator getJGroupsStack(String name) {
      return jgroupsBuilder.getStack(name);
   }

   public void setNamespaceMappingParser(NamespaceMappingParser namespaceMappingParser) {
      this.namespaceMappingParser = namespaceMappingParser;
   }

   @Override
   public void handleAnyElement(ConfigurationReader reader) {
      namespaceMappingParser.parseElement(reader, this);
   }

   @Override
   public void handleAnyAttribute(ConfigurationReader reader, int i) {
      namespaceMappingParser.parseAttribute(reader, i, this);
   }
}
