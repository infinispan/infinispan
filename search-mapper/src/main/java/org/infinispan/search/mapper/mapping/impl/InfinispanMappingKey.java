package org.infinispan.search.mapper.mapping.impl;

import org.hibernate.search.engine.mapper.mapping.building.spi.MappingKey;
import org.infinispan.search.mapper.log.impl.InfinispanEventContextMessages;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.jboss.logging.Messages;

public final class InfinispanMappingKey implements MappingKey<InfinispanMappingPartialBuildState, SearchMapping> {

   private static final InfinispanEventContextMessages MESSAGES =
         Messages.getBundle(InfinispanEventContextMessages.class);

   @Override
   public String render() {
      return MESSAGES.mapping();
   }
}
