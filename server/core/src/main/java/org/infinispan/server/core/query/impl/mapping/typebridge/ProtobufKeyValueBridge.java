package org.infinispan.server.core.query.impl.mapping.typebridge;

import java.io.IOException;

import org.hibernate.search.engine.backend.document.DocumentElement;
import org.hibernate.search.engine.backend.document.IndexObjectFieldReference;
import org.hibernate.search.mapper.pojo.bridge.TypeBridge;
import org.hibernate.search.mapper.pojo.bridge.runtime.TypeBridgeWriteContext;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.server.core.query.impl.indexing.IndexingTagHandler;
import org.infinispan.server.core.query.impl.logging.Log;
import org.infinispan.server.core.query.impl.mapping.reference.IndexReferenceHolder;
import org.infinispan.server.core.query.impl.mapping.type.ProtobufKeyValuePair;

public class ProtobufKeyValueBridge implements TypeBridge<ProtobufKeyValuePair> {

   private static final Log log = LogFactory.getLog(ProtobufKeyValueBridge.class, Log.class);

   private final IndexReferenceHolder indexReferenceHolder;
   private final String keyPropertyName;
   private final Descriptor keyDescriptor;
   private final Descriptor valueDescriptor;

   public ProtobufKeyValueBridge(IndexReferenceHolder indexReferenceHolder, String keyPropertyName,
                                 Descriptor keyDescriptor, Descriptor valueDescriptor) {
      this.indexReferenceHolder = indexReferenceHolder;
      this.keyPropertyName = keyPropertyName;
      this.keyDescriptor = keyDescriptor;
      this.valueDescriptor = valueDescriptor;
   }

   @Override
   public void write(DocumentElement target, ProtobufKeyValuePair bridgedElement, TypeBridgeWriteContext context) {
      IndexingTagHandler tagHandler = new IndexingTagHandler(valueDescriptor, target, indexReferenceHolder, null);
      try {
         ProtobufParser.INSTANCE.parse(tagHandler, valueDescriptor, bridgedElement.value());
      } catch (IOException e) {
         log.errorIndexingProtobufEntry(e);
      }

      if (keyDescriptor == null || keyPropertyName == null) {
         return;
      }

      IndexObjectFieldReference objectReference = indexReferenceHolder.getObjectReference(keyPropertyName);
      if (objectReference == null) {
         return;
      }

      tagHandler = new IndexingTagHandler(keyDescriptor, target.addObject(objectReference), indexReferenceHolder,
            keyPropertyName);
      try {
         ProtobufParser.INSTANCE.parse(tagHandler, keyDescriptor, bridgedElement.key());
      } catch (IOException e) {
         log.errorIndexingProtobufEntry(e);
      }
   }
}
