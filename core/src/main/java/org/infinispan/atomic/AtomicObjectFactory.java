package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.InvalidCacheUsageException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


/**
 * @author Pierre Sutra
 * @since 6.0
 */
public class AtomicObjectFactory {

    //
    // CLASS FIELDS
    //
    private static Log log = LogFactory.getLog(AtomicObjectFactory.class);

    //
    // OBJECT FIELDS
    //
	private Cache cache;
	private Map<Object,AtomicObjectContainer> registeredContainers;

    /**
     *
     * Return an AtomicObjectFactory.
     *
     * @param c a cache,  it must be synchronous.and transactional (with autoCommit set to true, its default value).
     */
	public AtomicObjectFactory(Cache<Object, Object> c) throws InvalidCacheUsageException{
        if( ! c.getCacheConfiguration().clustering().cacheMode().isSynchronous()
            || c.getAdvancedCache().getTransactionManager() == null )
            throw new InvalidCacheUsageException("The cache must be synchronous and transactional.");
		cache = c;
        registeredContainers= new HashMap<Object,AtomicObjectContainer>();
        log = LogFactory.getLog(this.getClass());
	}


    /**
     *
     * Returns an atomic object of class <i>clazz</i>.
     * The class of this object must be initially serializable, as well as all the parameters of its methods.
     * Furthermore the class must be deterministic.
     *
     * @param clazz a class object
     * @param key to use in order to store the object.
     * @return an object of the class <i>clazz</i>
     * @throws InvalidCacheUsageException
     */
 	public synchronized <T> T getInstanceOf(Class<T> clazz, Object key)
            throws InvalidCacheUsageException{
        return getInstanceOf(clazz, key, false);
	}
    /**
     *
     * Returns an object of class <i>clazz</i>.
     * The class of this object must be initially serializable, as well as all the parameters of its methods.
     * Furthermore the class must be deterministic.
     *
     * The object is atomic if <i>withReadOptimization</i> equals false; otherwise it is sequentially consistent..
     * In more details, if <i>withReadOptimization</i>  is set, every call to the object is first executed locally on a copy of the object, and in case
     * the call does not modify the state of the object, the value returned is the result of this tentative execution.
     *
     * @param clazz a class object
     * @param key the key to use in order to store the object.
     * @param withReadOptimization set the read optimization on/off.
     * @return an object of the class <i>clazz</i>
     * @throws InvalidCacheUsageException
     */
    public synchronized <T> T getInstanceOf(Class<T> clazz, Object key, boolean withReadOptimization)
            throws InvalidCacheUsageException{
        return getInstanceOf(clazz, key, withReadOptimization, null, true);
    }

    /**
     *
     * Returns an object of class <i>clazz</i>.
     * The class of this object must be initially serializable, as well as all the parameters of its methods.
     * Furthermore the class must be deterministic.
     *
     * The object is atomic if <i>withReadOptimization</i> equals false; otherwise it is sequentially consistent..
     * In more details, if <i>withReadOptimization</i>  is set, every call to the object is executed locally on a copy of the object, and in case
     * the call does not modify the state of the object, the value returned is the result of this tentative execution.
     * If the method <i>equalsMethod</i>  is not null, it overrides the default <i>clazz.equals()</i> when testing that the state of the object and
     * its copy are identical.
     *
     * @param clazz a class object
     * @param key the key to use in order to store the object.
     * @param withReadOptimization set the read optimization on/off.
     * @param equalsMethod overriding the default <i>clazz.equals()</i>.
     * @param forceNew force the creation of the object, even if it exists already in the cache
     * @return an object of the class <i>clazz</i>
     * @throws InvalidCacheUsageException
     */
    public synchronized <T> T getInstanceOf(Class<T> clazz, Object key, boolean withReadOptimization, Method equalsMethod, boolean forceNew)
            throws InvalidCacheUsageException{

        if( !(clazz instanceof Serializable)){
            throw new InvalidCacheUsageException("The object must be serializable.");
        }

        try{
            if(!registeredContainers.containsKey(key)){
                registeredContainers.put(key,new AtomicObjectContainer(cache, clazz, key, withReadOptimization, equalsMethod, forceNew));
            }
        } catch (Exception e){
            throw new InvalidCacheUsageException(e.getCause());
        }

        return (T) registeredContainers.get(key).getProxy();

    }

    /**
     * Remove the object stored at key from the local state.
     * If flag <i>keepPersistent</i> is set, a persistent copy of the current state of the object is stored in the cache.
     *
     * @param clazz a class object
     * @param key the key to use in order to store the object.
     * @param keepPersistent indicates that a persistent copy is stored in the cache or not.
     */
    public synchronized void disposeInstanceOf(Class clazz, Object key, boolean keepPersistent)
            throws IOException, InvalidCacheUsageException {

        AtomicObjectContainer container = registeredContainers.get(key);

        if( container == null )
            throw new InvalidCacheUsageException("The object does not exist.");

        if( ! container.getClazz().equals(clazz) )
            throw new InvalidCacheUsageException("The object is not of the right class.");

        try{
            container.dispose(keepPersistent);
        }catch (Exception e){
            throw new InvalidCacheUsageException(e.getCause());
        }

        registeredContainers.remove(key);

    }

    /**
     * @return a hash value of the order in which the calls on all the atomic objects built by this factory were executed.
     */
    @Override
    public int hashCode(){
        int ret = 0;
        for(AtomicObjectContainer c : registeredContainers.values())
            ret += c.hashCode();
        return ret;

    }

}
