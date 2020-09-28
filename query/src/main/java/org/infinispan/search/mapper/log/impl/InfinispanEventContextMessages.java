package org.infinispan.search.mapper.log.impl;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Message bundle for event contexts in the Infinispan Search mapper.
 */
@MessageBundle(projectCode = "ISPN")
public interface InfinispanEventContextMessages {

   @Message(value = "Infinispan Search Mapping")
   String mapping();

}
