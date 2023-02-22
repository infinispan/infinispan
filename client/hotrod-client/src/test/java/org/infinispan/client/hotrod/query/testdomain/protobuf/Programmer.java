package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Programmer {

   private String nick;

   private Integer contributions;

   @ProtoFactory
   public Programmer(String nick, Integer contributions) {
      this.nick = nick;
      this.contributions = contributions;
   }

   @Basic
   @ProtoField(value = 1)
   public String getNick() {
      return nick;
   }

   @Basic // not sortable!
   @ProtoField(value = 2)
   public Integer getContributions() {
      return contributions;
   }

   @AutoProtoSchemaBuilder(includeClasses = { Programmer.class }, schemaFileName = "pro.proto",
         schemaFilePath = "proto", schemaPackageName = "io.pro")
   public interface ProgrammerSchema extends GeneratedSchema {
   }
}
