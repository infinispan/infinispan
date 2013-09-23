package org.infinispan.ensemble;

import net.killa.kept.KeptConcurrentMap;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;



/**
 *
 * Zk is deployed on ALL microclouds
 * Each ucloud has an address to which it always answer (via DNS) to ease configuration here
 *
 * FIXME add watcher on the modifications that occur to the index.
 *
 * Do we implement a RemotecacheContainer (yes, if the laod is too heavy for Zk).
 * What pattern to use for handling faults properly ?
 * It is also possible that the manger are found on the fly using Zk
 *
 *
 * @author otrack
 * @since 4.0
 */
public class EnsembleCacheManager {

    private Map<UCloud,RemoteCacheManager> managers;
    private ConcurrentMap<String,List<UCloud>> index;
    private int replicationFactor;

    public EnsembleCacheManager(UCloud lucloud, List<UCloud> uclouds, int replicationFactor) throws IOException, KeeperException, InterruptedException {
        assert replicationFactor > uclouds.size();
        this.managers = new HashMap<UCloud, RemoteCacheManager>();
        this.replicationFactor = replicationFactor;
        for(UCloud ucloud : uclouds){
            RemoteCacheManager manager = new RemoteCacheManager(ucloud.getName());
            this.managers.put(ucloud, manager);
        }
        this.index = ZkManager.getInstance().newConcurrentMap("/index");
    }

    /**
     * Retrieve a cache object from the shared index.
     *
     * @param cacheName
     * @param <K>
     * @param <V>
     * @return null if no cache exists.
     */
    public <K,V> EnsembleCache<K,V> getCache(String cacheName){
        List<UCloud> uClouds = index.get(cacheName);
        if(uClouds==null) return null;
        List<ConcurrentMap<K,V>> caches = new ArrayList<ConcurrentMap<K,V>>();
        for(UCloud ucloud: uClouds){
            caches.add((RemoteCache<K, V>) managers.get(ucloud).getCache(cacheName));
        }
        return new EnsembleCache<K, V>(cacheName, caches);
    }

    /**
     *
     * Create a cache object in the shared index.
     * The object is retrieved in case, it was already existing.
     * The remote caches backing the object are assigned randomly
     *
     * This operation is not atomic.
     *
     * @param cacheName
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> EnsembleCache<K, V> putCache(String cacheName) {
        List<UCloud> uClouds = index.get(cacheName);
        if(uClouds==null){
            uClouds = assignRandomly();
            index.put(cacheName,uClouds);
        }
        return putCache(cacheName,uClouds);
    }

    /**
     *
     * Create a cache object in the shared index.
     * The object is retrieved in case, it was already existing.
     * The list of remote caches backing this object are specified.
     *
     * @param cacheName
     * @param uclouds
     * @param <K>
     * @param <V>
     * @return an EnsembleCache with name <i>cacheName</i>backed by RemoteCaches on <i>uclouds</i>.
     */
    public <K,V> EnsembleCache<K,V> putCache(String cacheName, List<UCloud> uclouds) {
        List<ConcurrentMap<K,V>> caches = new ArrayList<ConcurrentMap<K,V>>();
        for(UCloud ucloud: uclouds){
            caches.add((RemoteCache<K, V>) managers.get(ucloud).getCache(cacheName));
        }
        index.put(cacheName,uclouds);
        return new EnsembleCache<K, V>(cacheName, caches);
    }

    /**
     *
     * Remove the cache object from the shared index.
     *
     * @param cache
     * @param <K>
     * @param <V>
     * @return
     */
    public <K,V> boolean removeCache(EnsembleCache<K,V> cache){
        return index.remove(cache.getName()) == null;
    }


    //
    // INNER METHIDS
    //

    private UCloud retrieveUCloudByName(String name){
        for(UCloud ucloud : managers.keySet()){
            if(ucloud.getName().equals(name)) return  ucloud;
        }
        return null;
    }

    /**
     *
     * @return a list of <i>replicationFactor</i> uclouds.
     */
    private List<UCloud> assignRandomly(){
        List<UCloud> uclouds = new ArrayList<UCloud>(managers.keySet());
        java.util.Collections.shuffle(uclouds);
        for(int i=uclouds.size()-replicationFactor;i>0;i--)
            uclouds.remove(0);
        return uclouds;
    }

}
