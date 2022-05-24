package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(includeClasses = ModelA.class, schemaFileName = "model-schema.proto", schemaPackageName = "model")
public interface SchemaA extends GeneratedSchema {

   SchemaA INSTANCE = new SchemaAImpl();

}
