package org.infinispan.server.infinispan.actions;

import java.util.Set;

import org.jboss.as.clustering.infinispan.DefaultCacheContainer;

public class GetSitesViewAction extends AbstractDefaultCacheContainerAction<Set<String>> {

   public GetSitesViewAction(DefaultCacheContainer cacheManager) {
      super(cacheManager);
   }

   @Override
   public Set<String> run() {
      return cacheManager.getTransport().getSitesView();
   }

}
