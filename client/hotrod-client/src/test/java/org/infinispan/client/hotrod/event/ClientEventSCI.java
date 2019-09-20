package org.infinispan.client.hotrod.event;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.query.dsl.embedded.DslSCI;

@AutoProtoSchemaBuilder(
      dependsOn = DslSCI.class,
      includeClasses = {
            ClientEventsTest.CustomKey.class,
            ClientListenerWithFilterAndProtobufTest.CustomEventFilter.class,
            ClientListenerWithFilterAndRawProtobufTest.CustomEventFilter.class,
            CustomEventLogListener.CustomEvent.class,
            CustomEventLogListener.DynamicConverterFactory.DynamicConverter.class,
            CustomEventLogListener.FilterConverterFactory.FilterConverter.class,
            CustomEventLogListener.RawStaticConverterFactory.RawStaticConverter.class,
            CustomEventLogListener.NumericCallbackCounter.class,
            CustomEventLogListener.SimpleConverterFactory.SimpleConverter.class,
            CustomEventLogListener.StaticConverterFactory.StaticConverter.class,
            EventLogListener.DynamicCacheEventFilterFactory.DynamicCacheEventFilter.class,
            EventLogListener.RawStaticCacheEventFilterFactory.RawStaticCacheEventFilter.class,
            EventLogListener.StaticCacheEventFilterFactory.StaticCacheEventFilter.class,
      },
      schemaFileName = "test.client.event.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.client")
public interface ClientEventSCI extends SerializationContextInitializer {
   ClientEventSCI INSTANCE = new ClientEventSCIImpl();
}
