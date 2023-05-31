package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.JGroupsConfiguration.TRANSPORT;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.remoting.transport.Transport;

/**
 * @since 10.0
 */
public class JGroupsConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<JGroupsConfiguration> {
   private final AttributeSet attributes;

   private List<StackConfigurationBuilder> stackConfigurationBuilders = new ArrayList<>();
   private List<StackFileConfigurationBuilder> stackFileConfigurationBuilders = new ArrayList<>();
   private Set<String> buildersByName = new HashSet<>();

   JGroupsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = JGroupsConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public void validate() {
      stackConfigurationBuilders.forEach(s -> validate());
      stackFileConfigurationBuilders.forEach(s -> validate());
   }

   public StackConfigurationBuilder addStack(String name) {
      if (buildersByName.contains(name)) {
         throw CONFIG.duplicateJGroupsStack(name);
      }
      StackConfigurationBuilder stackConfigurationBuilder = new StackConfigurationBuilder(name, this);
      buildersByName.add(name);
      stackConfigurationBuilders.add(stackConfigurationBuilder);
      return stackConfigurationBuilder;
   }

   public StackFileConfigurationBuilder addStackFile(String name) {
      if (buildersByName.contains(name)) {
         throw CONFIG.duplicateJGroupsStack(name);
      }
      StackFileConfigurationBuilder stackFileConfigurationBuilder = new StackFileConfigurationBuilder(name, this);
      buildersByName.add(name);
      stackFileConfigurationBuilders.add(stackFileConfigurationBuilder);
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
   public JGroupsConfigurationBuilder read(JGroupsConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      buildersByName.clear();
      template.stackFiles().forEach(s -> addStackFile(s.name()).read(s, combine));
      template.stacks().forEach(s -> addStack(s.name()).read(s, combine));
      for(StackFileConfigurationBuilder b : stackFileConfigurationBuilders) {
         buildersByName.add(b.getConfigurator().getName());
      }
      for(StackConfigurationBuilder b : stackConfigurationBuilders) {
         buildersByName.add(b.getConfigurator().getName());
      }
      return this;
   }

   public boolean hasStack(String name) {
      return buildersByName.contains(name);
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
