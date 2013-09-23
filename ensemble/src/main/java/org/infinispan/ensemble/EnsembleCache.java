 package org.infinispan.ensemble;

 import org.infinispan.client.hotrod.RemoteCache;

 import java.util.Collection;
 import java.util.List;
 import java.util.Map;
 import java.util.Set;
 import java.util.concurrent.ConcurrentMap;

/**
 *
 * TODO change the atomic object factory to support a basic cache.
 * The atomicity of the cache will be thus part of the property on the put operation of the underlying cache.
 * Hence, this will be possible to use an assembled cache as input of the factory.
 *
 *
 * @author Pierre Sutra
 * @since 6.0
 */
public class EnsembleCache<K,V> implements ConcurrentMap<K,V> {

    private String name;
    private Collection<ConcurrentMap<K,V>> caches;
    private ConcurrentMap<K,V> primary;

    public EnsembleCache(String name, List<ConcurrentMap<K, V>> caches){
        this.name = name;
        this.caches = caches;
        this.primary = caches.iterator().next();
    }

    public void addCache(RemoteCache<K,V> cache){
         caches.add(cache);
    }

    public void removeCache(ConcurrentMap<K,V> cache){
        caches.remove(cache);
        if(cache == primary)
            primary = caches.iterator().next();
    }

    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return primary.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return primary.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object k) {
        return primary.containsKey(k);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object k) {
        return primary.containsValue(k);
    }

    /**
     * {@inheritDoc}
     *
     * Notice that if the replication factor is greater than 1, there is no consistency guarantee.
     * Otherwise, the consistency of the concerned cache applies.
     */
    @Override
    public V get(Object k) {
        return primary.get(k);

    }

    /**
     * {@inheritDoc}
     *
     * Notice that if the replication factor is greater than 1, there is no consistency guarantee.
     * Otherwise, the consistency of the concerned cache applies.
     */
    @Override
    public V put(K key, V value) {
        V ret = null;
        for(ConcurrentMap<K,V> c : caches)
            ret = c.put(key,value);
        return ret;
    }

    @Override
    public V remove(Object o) {
        return null;  // TODO: Customise this generated block
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for(ConcurrentMap<K,V> c: caches){
            c.putAll(map);
        }
    }

    @Override
    public void clear() {
        for(ConcurrentMap<K,V> c: caches){
            c.clear();
        }
    }

    @Override
    public Set<K> keySet() {
        return primary.keySet();
    }

    @Override
    public Collection<V> values() {
        return primary.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return primary.entrySet();
    }

    @Override
    public V putIfAbsent(K k, V v) {
        V ret = null;
        for(ConcurrentMap<K,V> c: caches){
            ret = (V) c.putIfAbsent(k, v);
        }
        return ret;
    }

    @Override
    public boolean remove(Object o, Object o2) {
        boolean ret = false;

        for(ConcurrentMap c: caches){
            ret |= c.remove(o,o2);
        }
        return ret;
    }

    @Override
    public boolean replace(K k, V v, V v2) {
        boolean ret = false;
        for(ConcurrentMap<K,V> c: caches){
            ret |= c.replace(k,v,v2);
        }
        return ret;
    }

    @Override
    public V replace(K k, V v) {
        V ret = null;
        for(ConcurrentMap<K,V> c: caches){
            ret = (V) c.replace(k,v);
        }
        return ret;
    }

}
