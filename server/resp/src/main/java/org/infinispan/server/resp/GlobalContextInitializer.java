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
            org.infinispan.server.resp.commands.tx.WATCH.TxEventConverterEmpty.class,
            org.infinispan.server.resp.filter.ComposedFilterConverter.class,
            org.infinispan.server.resp.filter.EventListenerConverter.class,
            org.infinispan.server.resp.filter.EventListenerKeysFilter.class,
            org.infinispan.server.resp.filter.GlobMatchFilterConverter.class,
            org.infinispan.server.resp.filter.RespTypeFilterConverter.class,
            org.infinispan.server.resp.json.JsonArrayAppendFunction.class,
            org.infinispan.server.resp.json.JsonArrindexFunction.class,
            org.infinispan.server.resp.json.JsonArrinsertFunction.class,
            org.infinispan.server.resp.json.JsonArrpopFunction.class,
            org.infinispan.server.resp.json.JsonArrtrimFunction.class,
            org.infinispan.server.resp.json.JsonBucket.class,
            org.infinispan.server.resp.json.JsonClearFunction.class,
            org.infinispan.server.resp.json.JsonDelFunction.class,
            org.infinispan.server.resp.json.JsonGetFunction.class,
            org.infinispan.server.resp.json.JsonLenArrayFunction.class,
            org.infinispan.server.resp.json.JsonLenObjFunction.class,
            org.infinispan.server.resp.json.JsonLenStrFunction.class,
            org.infinispan.server.resp.json.JsonMergeFunction.class,
            org.infinispan.server.resp.json.JsonNumIncrOpFunction.class,
            org.infinispan.server.resp.json.JsonNumMultOpFunction.class,
            org.infinispan.server.resp.json.JsonObjkeysFunction.class,
            org.infinispan.server.resp.json.JsonRespFunction.class,
            org.infinispan.server.resp.json.JsonSetFunction.class,
            org.infinispan.server.resp.json.JsonStringAppendFunction.class,
            org.infinispan.server.resp.json.JsonToggleFunction.class,
            org.infinispan.server.resp.json.JsonTypeFunction.class,
            org.infinispan.server.resp.RespTypes.class
      },
      schemaFileName = "global.resp.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.resp",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface GlobalContextInitializer extends SerializationContextInitializer { }
