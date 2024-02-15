package org.infinispan.search.mapper.search.loading.context.impl;

public class InfinispanSelectionLoadingBinder {
   public InfinispanSelectionLoadingStrategy createLoadingStrategy(){
      return InfinispanSelectionLoadingStrategy.instance();
   }
}
