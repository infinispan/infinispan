package org.infinispan.server.resp.commands.json;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_DOC)
public class JsonDoc {
   byte[] bytesDocument;
   @ProtoFactory
   JsonDoc(byte[] bytesDocument) {
      this.bytesDocument=bytesDocument;
   }

   @ProtoField(number = 1)
   byte[] bytesDocument() {
      return bytesDocument;
   }
}
