package org.infinispan.objectfilter.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      dependsOn = org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
      includeClasses = {
            FilterResultImpl.class,
            org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult.StatementType.class
      },
      schemaFileName = "global.objectfilter.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.objectfilter",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface GlobalContextInitializer extends SerializationContextInitializer {
}
