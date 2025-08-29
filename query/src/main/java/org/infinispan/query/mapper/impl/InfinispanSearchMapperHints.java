package org.infinispan.query.mapper.impl;

import java.lang.invoke.MethodHandles;

import org.hibernate.search.mapper.pojo.reporting.spi.MapperHints;
import org.hibernate.search.util.common.logging.impl.MessageConstants;
import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = MessageConstants.PROJECT_CODE)
public interface InfinispanSearchMapperHints extends MapperHints {

   InfinispanSearchMapperHints INSTANCE = Messages.getBundle(MethodHandles.lookup(), InfinispanSearchMapperHints.class);

   @Message("Cannot read Jandex Root Mapping")
   String cannotReadJandexRootMapping();

}
