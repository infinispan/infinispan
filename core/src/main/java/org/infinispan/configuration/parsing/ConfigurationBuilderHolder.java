package org.infinispan.configuration.parsing;

import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationReaderContext;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.JGroupsConfigurationBuilder;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;

public class ConfigurationBuilderHolder implements ConfigurationReaderContext {

   private final GlobalConfigurationBuilder globalConfigurationBuilder;
   private final Map<String, ConfigurationBuilder> namedConfigurationBuilders;
   private ConfigurationBuilder currentConfigurationBuilder;
   private final WeakReference<ClassLoader> classLoader;
   private final Deque<String> scope;
   private final JGroupsConfigurationBuilder jgroupsBuilder;
   private NamespaceMappingParser namespaceMappingParser;
   private final List<ConfigurationParserListener> listeners = new ArrayList<>();
   private Combine combine = Combine.DEFAULT;

   public ConfigurationBuilderHolder() {
      this(Thread.currentThread().getContextClassLoader());
   }

   public ConfigurationBuilderHolder(ClassLoader classLoader) {
      this(classLoader, new GlobalConfigurationBuilder().classLoader(classLoader));
   }

   public ConfigurationBuilderHolder(ClassLoader classLoader, GlobalConfigurationBuilder globalConfigurationBuilder) {
      this.globalConfigurationBuilder = globalConfigurationBuilder;
      this.namedConfigurationBuilders = new LinkedHashMap<>();
      this.jgroupsBuilder = this.globalConfigurationBuilder.transport().jgroups();
      this.classLoader = new WeakReference<>(classLoader);
      scope = new ArrayDeque<>();
      scope.push(ParserScope.GLOBAL.name());
   }

   public GlobalConfigurationBuilder global() {
      return globalConfigurationBuilder;
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

   public void read(ConfigurationBuilderHolder other) {
      globalConfigurationBuilder.read(other.globalConfigurationBuilder.build());
      for (Map.Entry<String, ConfigurationBuilder> entry : other.getNamedConfigurationBuilders().entrySet()) {
         namedConfigurationBuilders.computeIfAbsent(entry.getKey(), s -> new ConfigurationBuilder()).read(entry.getValue().build());
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

   public void setCombine(Combine combine) {
      this.combine = combine;
   }

   public Combine getCombine() {
      return combine;
   }

   public void addParserListener(ConfigurationParserListener listener) {
      listeners.add(listener);
   }

   public void fireParserListeners() {
      for (ConfigurationParserListener listener : listeners) {
         listener.parsingComplete(this);
      }
   }

   public ClassLoader getClassLoader() {
      return classLoader.get();
   }

   public ConfigurationBuilderHolder validate() {
      globalConfigurationBuilder.defaultCacheName().ifPresent(name -> {
         if (!namedConfigurationBuilders.containsKey(name))
            throw CONFIG.missingDefaultCacheDeclaration(name);
      });
      return this;
   }

   public void addJGroupsStack(FileJGroupsChannelConfigurator stack) {
      jgroupsBuilder.addStackFile(stack.getName()).fileChannelConfigurator(stack);
   }

   public void addJGroupsStack(EmbeddedJGroupsChannelConfigurator stack, String extend) {
      jgroupsBuilder.addStack(stack.getName()).extend(extend).channelConfigurator(stack);
   }

   boolean hasJGroupsStack(String name) {
      return jgroupsBuilder.hasStack(name);
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

   void resolveConfigurations() {
      // Resolve all configurations
      for (Map.Entry<String, ConfigurationBuilder> entry : namedConfigurationBuilders.entrySet()) {
         ConfigurationBuilder builder = resolveConfiguration(entry.getKey(), combine);
         entry.setValue(builder);
      }
   }

   private ConfigurationBuilder resolveConfiguration(String name, Combine combine) {
      ConfigurationBuilder builder = namedConfigurationBuilders.get(name);
      if (builder == null) {
         throw CONFIG.noConfiguration(name);
      }
      String parent = builder.configuration();
      if (parent == null) {
         builder.validate();
         // No parents, return as-is
         return builder;
      } else {
         ConfigurationBuilder rebased = new ConfigurationBuilder();
         rebased.read(resolveConfiguration(parent, combine).build(), combine);
         rebased.read(builder.build(false), combine);
         rebased.validate();
         return rebased;
      }
   }
}
