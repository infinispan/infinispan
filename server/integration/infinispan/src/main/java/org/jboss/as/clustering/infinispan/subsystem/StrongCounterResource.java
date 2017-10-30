package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 * /subsystem=infinispan/cache-container=X/counter=*
 *
 *
 * @author Vladimir Blagojevic
 * @since 9.2
 */
public class StrongCounterResource extends CounterResource {

   public static final PathElement PATH = PathElement.pathElement(ModelKeys.STRONG_COUNTER);

   static final SimpleAttributeDefinition LOWER_BOUND =
         new SimpleAttributeDefinitionBuilder(ModelKeys.LOWER_BOUND, ModelType.LONG, true)
         .setXmlName(Attribute.LOWER_BOUND.getLocalName())
         .setAllowExpression(false)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .build();

   static final SimpleAttributeDefinition UPPER_BOUND =
         new SimpleAttributeDefinitionBuilder(ModelKeys.UPPER_BOUND, ModelType.LONG, true)
         .setXmlName(Attribute.UPPER_BOUND.getLocalName())
         .setAllowExpression(false)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .build();

   static final SimpleAttributeDefinition[] STRONG_ATTRIBUTES = { LOWER_BOUND, UPPER_BOUND };

   public StrongCounterResource(boolean runtimeRegistration) {
      super(StrongCounterResource.PATH,
            new InfinispanResourceDescriptionResolver(ModelKeys.COUNTERS),
            new StrongCounterAddHandler(), new CounterRemoveHandler(), runtimeRegistration);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      super.registerAttributes(resourceRegistration);

      for (AttributeDefinition attr : STRONG_ATTRIBUTES) {
          resourceRegistration.registerReadOnlyAttribute(attr, null);
      }
   }
}
