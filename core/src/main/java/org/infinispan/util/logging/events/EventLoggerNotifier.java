package org.infinispan.util.logging.events;

import java.util.concurrent.CompletionStage;

import org.infinispan.notifications.Listenable;

public interface EventLoggerNotifier extends Listenable {

   /**
    * Notify the listeners about logged information. This method notifies about any type of logged information, without
    * filtering for level or category. Is up to the listeners to filter the desired events.
    *
    * @param log: the logged information.
    * @return a {@link CompletionStage} which completes when the notification has been sent.
    */
   CompletionStage<Void> notifyEventLogged(EventLog log);
}
