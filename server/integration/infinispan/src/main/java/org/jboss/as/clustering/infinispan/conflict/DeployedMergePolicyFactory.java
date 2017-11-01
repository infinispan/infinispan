package org.jboss.as.clustering.infinispan.conflict;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.EntryMergePolicyFactory;
import org.jboss.as.clustering.infinispan.InfinispanLogger;

public class DeployedMergePolicyFactory implements EntryMergePolicyFactory {

   private static final int TIMEOUT_SECONDS = 60;
   private final ConcurrentHashMap<String, CompletableFuture<DeployedMergePolicy>> policies = new ConcurrentHashMap<>();

   @Override
   public <T> T createInstance(PartitionHandlingConfiguration config) {
      EntryMergePolicy mergePolicy = config.mergePolicy();
      if (mergePolicy == null)
         return null;

      if (mergePolicy instanceof DeployedMergePolicy) {
         DeployedMergePolicy wrapper = (DeployedMergePolicy) mergePolicy;
         try {
            return (T) policies.computeIfAbsent(wrapper.getClassName(), k -> new CompletableFuture<>()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
         } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("An error occurred while processing the deployment", e);
         } catch (TimeoutException e) {
            InfinispanLogger.ROOT_LOGGER.loadingCustomMergePolicyTimeout(wrapper.getClassName());
            throw new CacheException(e);
         }
      }
      return (T) mergePolicy;
   }

   void addDeployedPolicy(DeployedMergePolicy wrapper) {
      policies.compute(wrapper.getClassName(), (k, cf) -> {
         if (cf != null && !cf.isDone()) {
            cf.complete(wrapper);
            return cf;
         }
         return CompletableFuture.completedFuture(wrapper);
      });
   }
}
