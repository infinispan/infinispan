package org.infinispan.client.hotrod.annotation.model;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@Indexed
public class Message {

   private Map<String, String> header;

   private String body;

   @ProtoField(value = 1, mapImplementation = HashMap.class)
   public Map<String, String> getHeader() {
      return header;
   }

   public void setHeader(Map<String, String> header) {
      this.header = header;
   }

   @ProtoField(2)
   @Text
   public String getBody() {
      return body;
   }

   public void setBody(String body) {
      this.body = body;
   }

   @ProtoSchema(syntax = ProtoSyntax.PROTO3, includeClasses = {Message.class}, schemaPackageName = "model")
   public interface MessageSchema extends GeneratedSchema {
      MessageSchema INSTANCE = new MessageSchemaImpl();
   }
}
