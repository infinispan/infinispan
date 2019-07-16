package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.JGroupsConfiguration.TRANSPORT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;

/**
 * @since 10.0
 */
public class JGroupsConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<JGroupsConfiguration> {
   private final AttributeSet attributes;

   private List<StackConfigurationBuilder> stackConfigurationBuilders = new ArrayList<>();
   private List<StackFileConfigurationBuilder> stackFileConfigurationBuilders = new ArrayList<>();
   private Map<String, StackBuilder<?>> buildersByName = new HashMap<>();

   JGroupsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = JGroupsConfiguration.attributeDefinitionSet();
   }

   @Override
   public void validate() {
      stackConfigurationBuilders.forEach(s -> validate());
      stackFileConfigurationBuilders.forEach(s -> validate());
   }

   public StackConfigurationBuilder addStack(String name) {
      StackConfigurationBuilder stackConfigurationBuilder = new StackConfigurationBuilder(name, this.getGlobalConfig());
      buildersByName.put(name, stackConfigurationBuilder);
      stackConfigurationBuilders.add(stackConfigurationBuilder);
      return stackConfigurationBuilder;
   }

   public StackFileConfigurationBuilder addStackFile(String name) {
      StackFileConfigurationBuilder stackFileConfigurationBuilder = new StackFileConfigurationBuilder(name, this.getGlobalConfig());
      stackFileConfigurationBuilders.add(stackFileConfigurationBuilder);
      buildersByName.put(name, stackFileConfigurationBuilder);
      return stackFileConfigurationBuilder;
   }

   public JGroupsConfigurationBuilder transport(Transport transport) {
      attributes.attribute(TRANSPORT).set(transport);
      return this;
   }

   public JGroupsConfigurationBuilder clear() {
      stackConfigurationBuilders = new ArrayList<>();
      stackFileConfigurationBuilders = new ArrayList<>();
      return this;
   }

   Transport jgroupsTransport() {
      return attributes.attribute(TRANSPORT).get();
   }

   @Override
   public JGroupsConfiguration create() {
      List<StackFileConfiguration> stackFileConfigurations = stackFileConfigurationBuilders.stream()
            .map(StackFileConfigurationBuilder::create).collect(Collectors.toList());
      List<StackConfiguration> stackConfigurations = stackConfigurationBuilders.stream()
            .map(StackConfigurationBuilder::create).collect(Collectors.toList());
      return new JGroupsConfiguration(attributes.protect(), stackFileConfigurations, stackConfigurations);
   }

   @Override
   public JGroupsConfigurationBuilder read(JGroupsConfiguration template) {
      attributes.read(template.attributes());
      template.stackFiles().forEach(s -> stackFileConfigurationBuilders.add(new StackFileConfigurationBuilder(s.name(), getGlobalConfig()).read(s)));
      template.stacks().forEach(s -> stackConfigurationBuilders.add(new StackConfigurationBuilder(s.name(), getGlobalConfig()).read(s)));
      return this;
   }

   public JGroupsChannelConfigurator getStack(String name) {
      StackBuilder<?> stackBuilder = buildersByName.get(name);
      return stackBuilder == null ? null : stackBuilder.getConfigurator(name);
   }

   @Override
   public String toString() {
      return "JGroupsConfigurationBuilder{" +
            "attributes=" + attributes +
            ", stackConfigurationBuilders=" + stackConfigurationBuilders +
            ", stackFileConfigurationBuilders=" + stackFileConfigurationBuilders +
            '}';
   }
}
