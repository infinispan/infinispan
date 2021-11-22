package org.infinispan.configuration.global;

import static org.infinispan.commons.configuration.attributes.IdentityAttributeCopier.identityCopier;

import java.util.List;
import java.util.stream.Stream;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;
import org.infinispan.util.logging.Log;

/*
 * @since 10.0
 */
public class JGroupsConfiguration {

   static final AttributeDefinition<Transport> TRANSPORT = AttributeDefinition
         .builder("transport", null, Transport.class)
         .copier(identityCopier()).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JGroupsConfiguration.class, TRANSPORT);
   }

   private final Attribute<Transport> transport;
   private final List<StackFileConfiguration> stackFileConfigurations;
   private final List<StackConfiguration> stackConfigurations;
   private final AttributeSet attributes;

   JGroupsConfiguration(AttributeSet attributes, List<StackFileConfiguration> stackFileConfigurations, List<StackConfiguration> stackConfigurations) {
      this.attributes = attributes.checkProtection();
      this.transport = attributes.attribute(TRANSPORT);
      this.stackFileConfigurations = stackFileConfigurations;
      this.stackConfigurations = stackConfigurations;
      this.stackConfigurations.forEach(c -> c.configurator().setConfiguration(this));
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public Transport transport() {
      return transport.get();
   }

   public List<StackFileConfiguration> stackFiles() {
      return stackFileConfigurations;
   }

   public List<StackConfiguration> stacks() {
      return stackConfigurations;
   }

   public boolean isClustered() {
      return transport.get() != null;
   }

   public JGroupsChannelConfigurator configurator(String name) {
      return Stream.concat(stackConfigurations.stream(), stackFileConfigurations.stream())
            .filter(s -> s.name().equals(name))
            .map(NamedStackConfiguration::configurator)
            .findFirst()
            .orElseThrow(() -> Log.CONFIG.missingJGroupsStack(name));
   }

   @Override
   public String toString() {
      return "JGroupsConfiguration{" +
            "transport=" + transport +
            ", stackFileConfigurations=" + stackFileConfigurations +
            ", stackConfigurations=" + stackConfigurations +
            '}';
   }

}
