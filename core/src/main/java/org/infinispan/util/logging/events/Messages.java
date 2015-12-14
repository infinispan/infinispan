package org.infinispan.util.logging.events;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

import static org.jboss.logging.Messages.getBundle;

/**
 * Messages.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MESSAGES = getBundle(Messages.class);

   @Message(value = "", id = 100000)
   String nodeJoined();

   @Message(value = "", id = 100001)
   String nodeLeft();

   @Message(value = "Started local rebalance", id = 100002)
   String rebalanceStarted();

   @Message(value = "Finished local rebalance", id = 100003)
   String rebalanceCompleted();
}
