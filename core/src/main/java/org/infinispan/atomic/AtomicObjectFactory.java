package org.infinispan.atomic;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.infinispan.Cache;
import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * TODO: It should be considered if we use the cache, or we create a new one.
 * TODO Improve concurrency inside (concurrent accesses to the maps), and outside (currently, create/remove accesses to an object must be synchronized).
 * TODO Memory management of final parameters (encapsulate the proxy object).
 *
 * @author otrack
 *
 */
@Listener
public class AtomicObjectFactory {

    //
    // CLASS FIELDS
    //

    private static Log log;
    private static final int MAGIC_PERSIST = 0;
	private static final MethodFilter mfilter = new MethodFilter() {
		@Override
		public boolean isHandled(Method arg0) {
			return true;
		}
	};

    //
    // OBJECT FIELDS
    //

	private Cache cache;
	private Map<Integer,FutureCall> registeredCalls;
	private Map<Object,Object> registeredObjects;
    private int hash = 0; // for debug purpose
    private AtomicInteger callCounter;

    /**
     *
     * Return an AtomicObjectFactory.
     *
     * @param c a cache; it must be synchronous.and transactional (with autoCommit set to true (default value).
     */
	public AtomicObjectFactory(Cache<Object, Object> c) throws InvalidCacheUsageException{
        if( ! c.getCacheConfiguration().clustering().cacheMode().isSynchronous()
            || c.getAdvancedCache().getTransactionManager() == null )
            throw new InvalidCacheUsageException("The cache must be synchronous.and transactional.");
		cache = c;
		registeredCalls = new ConcurrentHashMap<Integer, FutureCall>();
		registeredObjects = new ConcurrentHashMap<Object,Object>();
		cache.addListener(this);
        log = LogFactory.getLog(this.getClass());
        callCounter = new AtomicInteger(0);
	}


    /**
     *
     * Returns an atomic object of class <i>clazz</i>.
     * The class of this object must be initially serializable, as well as all the parameters of its methods.
     *
     * @param clazz a class object
     * @param key the key to use in order to store the object.
     * @return an object of the class <i>clazz</i>
     * @throws InvalidCacheUsageException
     */
 	public synchronized Object getOrCreateInstanceOf(Class<?> clazz, final Object key)
            throws InvalidCacheUsageException{
        return getOrCreateInstanceOf(clazz, key, false);
	}
    /**
     *
     * Returns an object of class <i>clazz</i>.
     *
     * The object is atomic if <i>withReadOptimization</i> equals false; otherwise it is sequentially consistent..
     * If <i>withReadOptimization</i>  is set, the call is executed locally on a copy of the object, and in case
     * the call does not modify the state of the object, the value returned is the result of this tentative execution.
     *
     * The class of this object must be initially serializable, as well as all the parameters of its methods.
     *
     * @param clazz a class object
     * @param key the key to use in order to store the object.
     * @param withReadOptimization set the read optimization on/off.
     * @return an object of the class <i>clazz</i>
     * @throws InvalidCacheUsageException
     */
    public synchronized Object getOrCreateInstanceOf(Class<?> clazz, final Object key, final boolean withReadOptimization)
            throws InvalidCacheUsageException{
        return getOrCreateInstanceOf(clazz,key,withReadOptimization,null);
    }

    /**
     *
     * Returns an object of class <i>clazz</i>.
     *
     * The object is atomic if <i>withReadOptimization</i> equals false; otherwise it is sequentially consistent..
     * If <i>withReadOptimization</i>  is set, the call is executed locally on a copy of the object, and in case
     * the call does not modify the state of the object, the value returned is the result of this tentative execution.
     * Method <i>equalsMethod</i> if not null overrides the default <i>clazz.equals()</i>
     *
     * The class of this object must be initially serializable, as well as all the parameters of its methods.
     *
     * @param clazz a class object
     * @param key the key to use in order to store the object.
     * @param withReadOptimization set the read optimization on/off.
     * @param  equalsMethod overriding the default <i>clazz.equals()</i>.
     * @return an object of the class <i>clazz</i>
     * @throws InvalidCacheUsageException
     */
    public synchronized Object getOrCreateInstanceOf(Class<?> clazz, final Object key, final boolean withReadOptimization, final Method equalsMethod)
            throws InvalidCacheUsageException{

        if( !(clazz instanceof Serializable)){
            throw  new NotSerializableException("The object must be serializable.");
        }

        if( registeredObjects.containsKey(key)){
            if( ! registeredObjects.get(key).getClass().equals(clazz) )
                throw new IllegalAccessError("An object with the same key but of a different class exists.");
            return registeredObjects.get(key);
        }

        Object rObject;

        try{

            if( cache.containsKey(key)){
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream((byte[]) cache.get(key)));
                int callID = ois.readInt();
                if( callID != MAGIC_PERSIST )
                   throw new InvalidCacheUsageException("Unable to retrieve persistent state.");
                rObject = ois.readObject();
            }else{
                rObject = clazz.newInstance();
            }
            registeredObjects.put(key, rObject);

            MethodHandler handler = new MethodHandler() {
                public Object invoke(Object self, Method m, Method proceed, Object[] args) throws Throwable {

                    ObjectOutputStream oos;
                    ByteArrayOutputStream baos;
                    ObjectInputStream ois;
                    ByteArrayInputStream bais;

                    if(withReadOptimization){
                        baos = new ByteArrayOutputStream();
                        oos = new ObjectOutputStream(baos);
                        oos.writeObject(registeredObjects.get(key));
                        oos.flush();
                        bais = new ByteArrayInputStream(baos.toByteArray());
                        ois = new ObjectInputStream(bais);
                        Object copy = ois.readObject();
                        Object ret = doCall(copy,m.getName(),args);
                        if( equalsMethod == null ? copy.equals(registeredObjects.get(key)) : equalsMethod.invoke(copy,registeredObjects).equals(Boolean.TRUE) )
                            return ret;
                    }

                    baos = new ByteArrayOutputStream();
                    oos = new ObjectOutputStream(baos);
                    int callId = m.hashCode()+Thread.currentThread().hashCode()+cache.getCacheManager().getAddress().hashCode()+callCounter.incrementAndGet();

                    oos.writeInt(callId);
                    oos.writeObject(m.getName());
                    oos.writeObject(args);
                    byte [] data = baos.toByteArray();

                    ByteBuffer bb = ByteBuffer.allocate(data.length);
                    bb.put(data);
                    bb.flip();

                    FutureCall future = new FutureCall();
                    registeredCalls.put(callId, future);

                    cache.put(key, bb.array());

                    Object ret = future.get();
                    registeredCalls.remove(callId);

                    log.debug("Call "+m.getName()+"() : :"+ret);
                    return ret;
                }
            };

            ProxyFactory fact = new ProxyFactory();
            fact.setSuperclass(clazz);
            fact.setFilter(mfilter);
            Class<?> pclazz = fact.createClass();
            Object pinst = pclazz.newInstance();
            ((ProxyObject)pinst).setHandler(handler);
            return pinst;

        } catch (Exception e){
            e.printStackTrace();
            throw new InvalidCacheUsageException(e.getCause());
        }

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

        if(!registeredObjects.containsKey(key))
            throw new InvalidCacheUsageException("The object does not exist.");

        Object object = registeredObjects.get(key);

        if( ! object.getClass().equals(clazz) )
            throw new InvalidCacheUsageException("The object is not of the right class.");

        if(!keepPersistent){
            cache.remove(key);
        }else{
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeInt(MAGIC_PERSIST);
            oos.writeObject(object);
            byte [] data = baos.toByteArray();
            ByteBuffer bb = ByteBuffer.allocate(data.length);
            bb.put(data);
            bb.flip();
            cache.put(key, bb.array());
        }

        registeredObjects.remove(key);

    }


    /**
     * Internal use of the listener API.
     *
     * @param event of class CacheEntryModifiedEvent
     */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@CacheEntryModified
    @Deprecated
	public synchronized void onCacheModification(CacheEntryModifiedEvent<Object,Object> event){

        if(event.isPre())
			return;

        try {
			Object rObject = registeredObjects.get(event.getKey());
            byte[] bb = (byte[]) event.getValue();
			ByteArrayInputStream bais = new ByteArrayInputStream(bb);
			ObjectInputStream ois = new ObjectInputStream(bais);
			int callId = ois.readInt();

            if(registeredCalls.containsKey(callId) && !event.isOriginLocal()) // an event can be received twice if it is local
                return;

            log.debug("Received call with ID=" + callId + " from " + event.getCache().toString());

            if(callId == MAGIC_PERSIST)
                return;

			String method = (String)ois.readObject();
            Object[] args = (Object[])ois.readObject();
            Object ret = doCall(rObject,method,args);
            hash+=callId;

			if(registeredCalls.containsKey(callId)){
				registeredCalls.get(callId).setReturnValue(ret);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

    /**
     * Interrnal use.
     * @return a hash of the order in whch the calls sto the objects where executed.
     */
    int getHash(){
        return hash;
    }

    private Object doCall(Object rObject, String method, Object[] args) throws InvocationTargetException, IllegalAccessException {
        boolean isFound = false;
        Object ret = null;
        for (Method m : rObject.getClass().getMethods()) { // only public methods (inherited and not)
            if (method.equals(m.getName())) {
                boolean isAssignable = true;
                Class[] argsTypes = m.getParameterTypes();
                if(argsTypes.length == args.length){
                    for(int i=0; i<argsTypes.length; i++){
                        if( !argsTypes[i].isAssignableFrom(args[i].getClass()) ){
                            isAssignable = false;
                            break;
                        }
                    }
                }
                if(!isAssignable)
                    continue;

                ret = m.invoke(rObject, args);
                isFound = true;
                break;
            }
        }

        if(!isFound)
            throw new IllegalStateException("Method "+method+" not found.");

        return ret;
    }
	
	//
	// Inner Classes
	//
	
	private class FutureCall implements Future<Object> {

		private Object ret;
		private int state; // 0 => init, 1 => done, -1 => cancelled
		
		public FutureCall(){
			ret = null;
			state = 0;
		}
		
		public void setReturnValue(Object r){
			synchronized (this) {

				if (state == -1)
					return ;

				if (ret == null) {
					ret = r;
					state = 1;
					this.notifyAll();
					return;
				} 
			}
				
			throw new IllegalStateException("Unreachable code");
		}
		
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			synchronized (this) {
				if (state != 0)
					return false;
				state = -1;
				if (mayInterruptIfRunning)
					this.notifyAll();
			}
			return true;
		}

		@Override
		public Object get() throws InterruptedException, ExecutionException {
			synchronized (this) {
				if (state == 0)
					this.wait();
			}
			return (state == -1) ? null : ret;
		}

		@Override
		public Object get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			synchronized (this) {
				if (state == 0)
					this.wait(timeout);
			}
			return (state == -1) ? null : ret;
		}

		@Override
		public boolean isCancelled() {
			return state == -1;
		}

		@Override
		public boolean isDone() {
			return ret != null;
		}

	}


}
