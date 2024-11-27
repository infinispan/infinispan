package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(includeClasses = ModelA.class, schemaFileName = "model-schema.proto", schemaPackageName = "model", service = false)
public interface SchemaA extends GeneratedSchema {

   SchemaA INSTANCE = new SchemaAImpl();

}
