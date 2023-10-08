package org.infinispan.server.resp;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            PersistenceContextInitializer.class,
            org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
            org.infinispan.server.core.GlobalContextInitializer.class,
      },
      includeClasses = {
            org.infinispan.server.resp.commands.tx.WATCH.class,
            org.infinispan.server.resp.commands.tx.WATCH.TxEventConverterEmpty.class,
            org.infinispan.server.resp.filter.ComposedFilterConverter.class,
            org.infinispan.server.resp.filter.EventListenerConverter.class,
            org.infinispan.server.resp.filter.EventListenerKeysFilter.class,
            org.infinispan.server.resp.filter.GlobMatchFilterConverter.class,
            org.infinispan.server.resp.filter.RespTypeFilterConverter.class,
      },
      schemaFileName = "global.resp.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.resp",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface GlobalContextInitializer extends SerializationContextInitializer { }
