package org.infinispan.query.impl;

import org.hibernate.search.engine.environment.bean.BeanReference;
import org.hibernate.search.mapper.pojo.bridge.IdentifierBridge;
import org.hibernate.search.mapper.pojo.bridge.runtime.IdentifierBridgeFromDocumentIdentifierContext;
import org.hibernate.search.mapper.pojo.bridge.runtime.IdentifierBridgeToDocumentIdentifierContext;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * An {@link IdentifierBridge} to map cache keys to the document identifiers.
 *
 * @author Fabio Massimo Ercoli
 */
public class KeyTransformationHandlerIdentifierBridge implements IdentifierBridge<Object> {

   public static BeanReference<? extends IdentifierBridge<Object>> createReference(
         KeyTransformationHandler keyTransformationHandler) {
      return BeanReference.ofInstance(new KeyTransformationHandlerIdentifierBridge(keyTransformationHandler));
   }

   private final KeyTransformationHandler keyTransformationHandler;

   private KeyTransformationHandlerIdentifierBridge(KeyTransformationHandler keyTransformationHandler) {
      this.keyTransformationHandler = keyTransformationHandler;
   }

   @Override
   public String toDocumentIdentifier(Object propertyValue, IdentifierBridgeToDocumentIdentifierContext context) {
      return keyTransformationHandler.keyToString(propertyValue);
   }

   @Override
   public Object fromDocumentIdentifier(String documentIdentifier, IdentifierBridgeFromDocumentIdentifierContext context) {
      return keyTransformationHandler.stringToKey(documentIdentifier);
   }
}
