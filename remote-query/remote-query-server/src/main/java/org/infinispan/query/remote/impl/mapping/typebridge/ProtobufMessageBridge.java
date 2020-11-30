package org.infinispan.query.remote.impl.mapping.typebridge;

import java.io.IOException;

import org.hibernate.search.engine.backend.document.DocumentElement;
import org.hibernate.search.mapper.pojo.bridge.TypeBridge;
import org.hibernate.search.mapper.pojo.bridge.runtime.TypeBridgeWriteContext;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.impl.indexing.IndexingTagHandler;
import org.infinispan.query.remote.impl.mapping.reference.IndexReferenceHolder;

public class ProtobufMessageBridge implements TypeBridge<byte[]> {

   private final IndexReferenceHolder indexReferenceHolder;
   private final Descriptor descriptor;

   ProtobufMessageBridge(IndexReferenceHolder indexReferenceHolder, Descriptor descriptor) {
      this.indexReferenceHolder = indexReferenceHolder;
      this.descriptor = descriptor;
   }

   @Override
   public void write(DocumentElement target, byte[] messageBytes, TypeBridgeWriteContext context) {
      IndexingTagHandler tagHandler = new IndexingTagHandler(descriptor, target, indexReferenceHolder);

      try {
         ProtobufParser.INSTANCE.parse(tagHandler, descriptor, messageBytes);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }
}
