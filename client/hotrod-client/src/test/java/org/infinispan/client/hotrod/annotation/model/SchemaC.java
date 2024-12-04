package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(includeClasses = { ModelC.class, Image.class }, schemaFileName = "model-schema.proto", schemaPackageName = "model", service = false)
public interface SchemaC extends GeneratedSchema {

   SchemaC INSTANCE = new SchemaCImpl();

}
