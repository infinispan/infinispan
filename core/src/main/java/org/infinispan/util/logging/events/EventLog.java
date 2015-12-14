package org.infinispan.util.logging.events;

import java.time.Instant;
import java.util.Optional;

/**
 * EventLog describes an event log's attributes.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public interface EventLog {
   /**
    * @return the instant when the event occurred
    */
   Instant getWhen();

   /**
    * @return the level of this event's severity
    */
   EventLogLevel getLevel();

   /**
    * @return the message of the event.
    */
   String getMessage();

   /**
    * @return the category of the event
    */
   EventLogCategory getCategory();

   /**
    * @return the detail of the event, e.g. a stack trace.
    */
   Optional<String> getDetail();

   /**
    * @return the name of the principal if the event occurred within a security context.
    */
   Optional<String> getWho();

   /**
    * @return the context of the event (e.g. the name of a cache).
    */
   Optional<String> getContext();

   /**
    * @return the scope of the event. If the event is specific to a node in the cluster, then this
    *         will be the node's address. If the event is global to the entire cluster this will be
    *         {@link Optional#empty()}
    */
   Optional<String> getScope();
}
