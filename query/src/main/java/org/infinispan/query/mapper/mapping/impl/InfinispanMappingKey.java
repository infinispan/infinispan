package org.infinispan.query.mapper.mapping.impl;

import java.lang.invoke.MethodHandles;

import org.hibernate.search.engine.mapper.mapping.building.spi.MappingKey;
import org.infinispan.query.mapper.log.impl.InfinispanEventContextMessages;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.jboss.logging.Messages;

public final class InfinispanMappingKey implements MappingKey<InfinispanMappingPartialBuildState, SearchMapping> {

   private static final InfinispanEventContextMessages MESSAGES =
         Messages.getBundle(MethodHandles.lookup(), InfinispanEventContextMessages.class);

   @Override
   public String render() {
      return MESSAGES.mapping();
   }
}
