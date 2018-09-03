package org.infinispan.persistence.manager;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Separate the preload into its own component
 */
@Scope(Scopes.NAMED_CACHE)
public class PreloadManager {
   @Inject private PersistenceManager persistenceManager;

   @Start
   public void start() {
      persistenceManager.preload();
   }
}
