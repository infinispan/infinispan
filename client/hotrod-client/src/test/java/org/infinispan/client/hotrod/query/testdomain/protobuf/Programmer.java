package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed
public class Programmer {

   private final String nick;

   private final Integer contributions;

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

   @ProtoSchema(
         includeClasses = Programmer.class,
         schemaFileName = "pro.proto",
         schemaFilePath = "org/infinispan/client/hotrod",
         schemaPackageName = "io.pro",
         service = false
   )
   public interface ProgrammerSchema extends GeneratedSchema {
   }
}
