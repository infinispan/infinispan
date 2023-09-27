package org.infinispan.xsite.events;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A manager class that handles the {@link XSiteEvent} from local and remote sites.
 *
 * @since 15.0
 */
public interface XSiteEventsManager {

   /**
    * Handles a list of {@link XSiteEvent} from another node in the local site.
    *
    * @param events The {@link XSiteEvent} list.
    * @return A {@link CompletionStage} that is completed when all the events are processed.
    */
   CompletionStage<Void> onLocalEvents(List<XSiteEvent> events);

   /**
    * Handles a list of {@link XSiteEvent} from another site.
    *
    * @param events The {@link XSiteEvent} list.
    * @return A {@link CompletionStage} that is completed when all the events are processed.
    */
   CompletionStage<Void> onRemoteEvents(List<XSiteEvent> events);
}
