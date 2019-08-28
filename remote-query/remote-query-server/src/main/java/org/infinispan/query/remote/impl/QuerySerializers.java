package org.infinispan.query.remote.impl;

import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.dataconversion.MediaType;

final class QuerySerializers {

   private final ConcurrentHashMap<String, QuerySerializer<?>> serializers = new ConcurrentHashMap<>(2);

   QuerySerializers() {
   }

   void addSerializer(MediaType mediaType, QuerySerializer<?> querySerializer) {
      serializers.put(mediaType.getTypeSubtype(), querySerializer);
   }

   QuerySerializer<?> getSerializer(MediaType requestMediaType) {
      QuerySerializer<?> querySerializer = serializers.get(requestMediaType.getTypeSubtype());
      if (querySerializer != null) return querySerializer;

      return serializers.get(MediaType.MATCH_ALL_TYPE);
   }
}
