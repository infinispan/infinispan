package org.infinispan.query.impl;

import org.hibernate.search.engine.environment.bean.BeanReference;
import org.hibernate.search.mapper.pojo.bridge.IdentifierBridge;
import org.hibernate.search.mapper.pojo.bridge.runtime.IdentifierBridgeFromDocumentIdentifierContext;
import org.hibernate.search.mapper.pojo.bridge.runtime.IdentifierBridgeToDocumentIdentifierContext;

/**
 * An {@link IdentifierBridge} to map cache keys to the document identifiers.
 *
 * @author Fabio Massimo Ercoli
 */
public class CacheIdentifierBridge implements IdentifierBridge<Object> {

   public static BeanReference<? extends IdentifierBridge<Object>> getReference() {
      return BeanReference.ofInstance(new CacheIdentifierBridge());
   }

   private CacheIdentifierBridge() {
   }

   @Override
   public String toDocumentIdentifier(Object propertyValue, IdentifierBridgeToDocumentIdentifierContext context) {
      // TODO HSEARCH-3891 Pass segment id as key transfromation handler
      // with that we will able to use the keyTransformationHandler here,
      // instead of in the QueryInterceptor.
      return (String) propertyValue;
   }

   @Override
   public Object fromDocumentIdentifier(String documentIdentifier, IdentifierBridgeFromDocumentIdentifierContext context) {
      // TODO HSEARCH-3891 Pass segment id as key transfromation handler
      // with that we will able to use the keyTransformationHandler here,
      // instead of in the EntityLoader.
      return documentIdentifier;
   }
}
