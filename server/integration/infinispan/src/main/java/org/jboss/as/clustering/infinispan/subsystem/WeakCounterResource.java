package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 * /subsystem=infinispan/cache-container=X/counter=*
 *
 * @author Pedro Ruivo
 * @author Vladimir Blagojevic
 * @since 9.2
 */
public class WeakCounterResource extends CounterResource {

   public static final PathElement PATH = PathElement.pathElement(ModelKeys.WEAK_COUNTER);

   static final SimpleAttributeDefinition CONCURRENCY_LEVEL = new SimpleAttributeDefinitionBuilder(ModelKeys.CONCURRENCY_LEVEL,
         ModelType.INT, true)
           .setXmlName(Attribute.CONCURRENCY_LEVEL.getLocalName())
           .setAllowExpression(false)
           .setDefaultValue(new ModelNode().set(64))
           .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES).build();

   static final AttributeDefinition[] WEAK_ATTRIBUTES = { CONCURRENCY_LEVEL };

   public WeakCounterResource(boolean runtimeRegistration) {
      super(WeakCounterResource.PATH, new InfinispanResourceDescriptionResolver(ModelKeys.COUNTERS),
            new WeakCounterAddHandler(), new CounterRemoveHandler(), runtimeRegistration);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      super.registerAttributes(resourceRegistration);

      for (AttributeDefinition attr : WEAK_ATTRIBUTES) {
         resourceRegistration.registerReadOnlyAttribute(attr, null);
      }
   }
}
