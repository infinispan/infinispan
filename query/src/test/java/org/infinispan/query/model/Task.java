package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@Indexed
public record Task(@Basic(projectable = true) int id, String type, @Keyword String status,
                   @Basic(aggregable = true) String label) {

   @ProtoSchema(
         includeClasses = { Task.class },
         schemaFileName = "task.model",
         schemaPackageName = "model",
         syntax = ProtoSyntax.PROTO3,
         service = false
   )
   public interface TaskSchema extends GeneratedSchema {
      TaskSchema INSTANCE = new TaskSchemaImpl();
   }
}
