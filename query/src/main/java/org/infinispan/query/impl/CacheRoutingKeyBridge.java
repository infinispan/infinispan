package org.infinispan.query.impl;

import org.hibernate.search.mapper.pojo.bridge.RoutingKeyBridge;
import org.hibernate.search.mapper.pojo.bridge.binding.RoutingKeyBindingContext;
import org.hibernate.search.mapper.pojo.bridge.mapping.programmatic.RoutingKeyBinder;
import org.hibernate.search.mapper.pojo.bridge.runtime.RoutingKeyBridgeToRoutingKeyContext;

public class CacheRoutingKeyBridge implements RoutingKeyBridge {

   @Override
   public String toRoutingKey(String tenantIdentifier, Object entityIdentifier, Object bridgedElement,
                              RoutingKeyBridgeToRoutingKeyContext context) {
      // The value has been already converted before it,
      // using the keyTransformationHandler passing it the right segment id.
      // We don't have the segment id here. See:
      // TODO HSEARCH-3891 Pass segment id as key transfromation handler
      String documentId = (String) entityIdentifier;

      // extract the segment id
      return documentId.substring(documentId.lastIndexOf(":")+1);
   }

   public static class Binder implements RoutingKeyBinder {
      @Override
      public void bind(RoutingKeyBindingContext context) {
         context.getDependencies().useRootOnly();
         context.setBridge( new CacheRoutingKeyBridge() );
      }
   }
}
