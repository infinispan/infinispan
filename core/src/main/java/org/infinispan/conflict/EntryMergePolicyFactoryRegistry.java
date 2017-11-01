package org.infinispan.conflict;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * A registry for {@link EntryMergePolicyFactory} implementations, which allows {@link EntryMergePolicy} implementations
 * to be eagerly/lazily loaded across multiple contexts. The order in which {@link EntryMergePolicyFactory}
 * implementations are added to the registry determines their priority, with {@link EntryMergePolicyFactoryRegistry#createInstance(PartitionHandlingConfiguration)}
 * returning as soon as the first non-null implementation is encountered.
 *
 * In embedded mode we only expect a single factory implementation to be present as custom policy implementations are
 * provided during runtime by the user or loaded via the {@link org.infinispan.configuration.parsing.Parser}'s
 * {@link org.infinispan.configuration.cache.ConfigurationBuilder}'s classloader. However, in server mode it's possible
 * for users to deploy their custom policies to the server or use one of the default policies, therefore it's necessary
 * for both the embedded factory and a server factory to be utilised.
 */
@Scope(Scopes.GLOBAL)
public class EntryMergePolicyFactoryRegistry {

   private final List<EntryMergePolicyFactory> factories = new ArrayList<>();

   public EntryMergePolicyFactoryRegistry() {
      // Create the factory for local embedded classes
      factories.add(new EntryMergePolicyFactory() {
         @Override
         public <T> T createInstance(PartitionHandlingConfiguration config) {
            return (T) config.mergePolicy();
         }
      });
   }

   public EntryMergePolicy createInstance(PartitionHandlingConfiguration config) {
      for (EntryMergePolicyFactory factory : factories) {
         Object instance = factory.createInstance(config);
         if(instance != null)
            return (EntryMergePolicy) instance;
      }
      return null;
   }

   public void addMergePolicyFactory(EntryMergePolicyFactory factory) {
      Objects.requireNonNull(factory);
      factories.add(0, factory);
   }
}
