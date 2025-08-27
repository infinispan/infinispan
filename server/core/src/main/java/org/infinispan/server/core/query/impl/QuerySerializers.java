package org.infinispan.server.core.query.impl;

import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.dataconversion.MediaType;

public final class QuerySerializers {

   private final ConcurrentHashMap<String, QuerySerializer<?>> serializers = new ConcurrentHashMap<>(2);

   public QuerySerializers() {
   }

   public void addSerializer(MediaType mediaType, QuerySerializer<?> querySerializer) {
      serializers.put(mediaType.getTypeSubtype(), querySerializer);
   }

   QuerySerializer<?> getSerializer(MediaType requestMediaType) {
      QuerySerializer<?> querySerializer = serializers.get(requestMediaType.getTypeSubtype());
      if (querySerializer != null) return querySerializer;

      return serializers.get(MediaType.MATCH_ALL_TYPE);
   }
}
