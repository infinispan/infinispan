package org.infinispan.atomic;

import javassist.CannotCompileException;
import javassist.NotFoundException;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.infinispan.Cache;
import org.infinispan.InvalidCacheUsageException;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * TODO: It should be considered if we use the cache, or we create a new one.
 * TODO Implement a disposal system to prune an object from the cache.
 * TODO; The total order property of the underlying communication system is mandatory for atomicity.
 * @author otrack
 * T
 */
@Listener
public class AtomicTypeFactory{

	private final MethodFilter mfilter = new MethodFilter() {
		@Override
		public boolean isHandled(Method arg0) {
			return true;
		}
	};

	private Cache<Object,Object> cache;

	private Map<Integer,FutureCall> registeredCalls;
	
	private Map<Object,Object> registeredObjects;

    private Log log;

    private int hash = 0;

    private AtomicInteger callCounter;

    /**
     *
     * Return an AtomicTypeFactory object.
     *
     * @param c a cache; it must be synchronous.and transactional (with autoCommit set to true (default value).
     */
	public AtomicTypeFactory(Cache<Object,Object> c){
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
     * Returns an atomic object of the type <i>clazz</i>.
     * The class of this object must be initially serializable, as well as all the parameters of its methods.
     *
     * @param clazz a class object
     * @param key the key to use in order to store the object.
     * @return
     * @throws NotFoundException
     * @throws CannotCompileException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
	public Object newInstanceOf(Class<?> clazz, final Object key)
            throws NotFoundException, CannotCompileException, InstantiationException, IllegalAccessException{

		Object rObject = clazz.newInstance();
		registeredObjects.put(key, rObject);

		ProxyFactory fact = new ProxyFactory();
		fact.setSuperclass(clazz);
		fact.setFilter(mfilter);
		Class<?> pclazz = fact.createClass();
		
		MethodHandler handler = new MethodHandler() {
			public Object invoke(Object self, Method m, Method proceed, Object[] args) throws Throwable {

				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);
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

		Object pinst = pclazz.newInstance();
		((ProxyObject)pinst).setHandler(handler);
				
		return pinst;

	}


    /**
     * Internal use of the listener API.
     * @param event of type CacheEntryModifiedEvent
     */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@CacheEntryModified
    @Deprecated
	public synchronized void onCacheModification(CacheEntryModifiedEvent<Object,Object> event){

        if(event.isPre())
			return;

        if(!registeredObjects.containsKey(event.getKey()))
			return;

        try {
			Object rObject = registeredObjects.get(event.getKey());
            byte[] bb = (byte[]) event.getValue();
			ByteArrayInputStream bais = new ByteArrayInputStream(bb);
			ObjectInputStream ois = new ObjectInputStream(bais);
			int callId = ois.readInt();
			String method = (String)ois.readObject();

            if(registeredCalls.containsKey(callId) && !event.isOriginLocal())     // to deal with the fact that an event can be received twice. when it is local
               return;

            log.debug(event.toString()+" "+registeredObjects.toString());

            Object ret = null;
			Object[] args = (Object[])ois.readObject();
            boolean isFound = false;
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

                    log.debug("Received call with ID=" + callId + " from " + event.getCache().toString());
                    hash+=callId;

                    ret = m.invoke(rObject, args);
                    isFound = true;
	            	break;
	            }
	        }	        
	        
	        if(!isFound)
	        	throw new IllegalStateException("Method "+method+" not found.");
	        
			if(registeredCalls.containsKey(callId)){
				registeredCalls.get(callId).setReturnValue(ret);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

    /**
     * Interrnal use.
     * @return a hash of the order in whch the call to the objects where done.
     */
    @Deprecated
    public int getHash(){
        return hash;
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
