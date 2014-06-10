package org.infinispan.registry;

import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.KeyFilter;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A registry of scoped key-values available to all the nodes in the cluster.
 * Intended as a general purpose tool for sharing metadata: small amounts of information that is
 * not frequently updated. It is not intended to be used for sharing large amount
 * of information nor to be exposed to users.
 * <p/>
 * The registry is a global component, i.e. it is shared by all the caches in the cluster.
 * In order to avoid collisions each key-value entry is scoped (the <i>scope</i> parameter).
 * For scoping one can use fully qualified names of the specific classes/packages that make use
 * of the ClusterRegistry.
 * <p/>
 * A reference to the ClusterRegistry can be obtained either through the \@Inject annotation:
 * <pre>
 *    \@Inject
 *    void setup(ClusterRegistry cs) {
 *       ..hold the reference
 *    }
 * </pre>
 * or directly form the GlobalComponentRegistry:
 * <pre>
 *    EmbeddedCacheManager:getGlobalComponentRegistry():getGlobalComponent(ClusterRegistry.class)
 * </pre>
 *
 * @author Mircea Markus
 * @since 6.0
 */
@Scope(Scopes.GLOBAL)
public interface ClusterRegistry<S, K, V> {

   void put(S scope, K key, V value);

   void put(S scope, K key, V value, long lifespan, TimeUnit unit);

   void remove(S scope, K key);

   V get(S scope, K key);

   boolean containsKey(S scope, K key);

   Set<K> keys(S scope);

   void clear(S scope);

   void clearAll();

   /**
    * Adds a listener that is notified of changes to keys in a given scope.
    *
    * @param scope the scope to watch
    * @param listener a properly annotated listener instance
    */
   void addListener(S scope, Object listener);

   /**
    * Adds a listener that is notified of changes to keys in a given scope and which match a given {@code KeyFilter}.
    *
    * @param scope the scope to watch
    * @param listener a properly annotated listener instance
    */
   void addListener(S scope, KeyFilter keyFilter, Object listener);

   /**
    * Detaches a listener instance.
    *
    * @param listener the listener to detach
    */
   void removeListener(Object listener);
}
