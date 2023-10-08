package org.infinispan.jcache.embedded;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
      },
      includeClasses = {
            org.infinispan.jcache.embedded.functions.GetAndPut.class,
            org.infinispan.jcache.embedded.functions.GetAndRemove.class,
            org.infinispan.jcache.embedded.functions.GetAndReplace.class,
            org.infinispan.jcache.embedded.functions.Invoke.class,
            org.infinispan.jcache.embedded.functions.MutableEntrySnapshot.class,
            org.infinispan.jcache.embedded.functions.Put.class,
            org.infinispan.jcache.embedded.functions.PutIfAbsent.class,
            org.infinispan.jcache.embedded.functions.ReadWithExpiry.class,
            org.infinispan.jcache.embedded.functions.Remove.class,
            org.infinispan.jcache.embedded.functions.RemoveConditionally.class,
            org.infinispan.jcache.embedded.functions.Replace.class,
            org.infinispan.jcache.embedded.functions.ReplaceConditionally.class
      },
      schemaFileName = "global.jcache.embedded.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.jcache.embedded",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
interface GlobalContextInitializer extends SerializationContextInitializer {
}
