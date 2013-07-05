package org.infinispan.atomic;

import javassist.CannotCompileException;
import javassist.NotFoundException;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.infinispan.Cache;
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

/**
 * One solution: the proxy is a cache listener and we use the local instance of the object to push operations
 * inside ABCast; once the proxy receives the call it pushes last (allows sync and async calls), it returns the value.
 *
 * Another solution: the proxy delegates all calls to an instance located at some replica, 
 * and which is created if a certain magic string is inserted.
 *
 *
 * TODO: It should be considered if we use the cache, or we create a new one., in particular, because creating the cache is typed, and we need storing information with a priori not the right type.
 * TODO Implement a disposal system to prune an object from the cache.
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
	
	public AtomicTypeFactory(Cache<Object,Object> c){
		cache = c;
		registeredCalls = new ConcurrentHashMap<Integer, FutureCall>();
		registeredObjects = new ConcurrentHashMap<Object,Object>();
		cache.addListener(this);
        log = LogFactory.getLog(this.getClass());
	}

	public Object newInstanceOf(Class<?> clazz, final Object key) throws NotFoundException, CannotCompileException, InstantiationException, IllegalAccessException{

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
				int callId = m.hashCode()+Thread.currentThread().hashCode();
				
				oos.writeInt(callId);
				oos.writeObject(m.getName());
				oos.writeObject(args);
				byte [] data = baos.toByteArray();
				ByteBuffer bb = ByteBuffer.allocate(data.length);
				bb.put(data);
				bb.flip();
								
				FutureCall future = new FutureCall();
				registeredCalls.put(callId,future);
				cache.put(key, bb);
				
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
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@CacheEntryModified
	public void onCacheModification(CacheEntryModifiedEvent<Object,Object> event){
				
		if(event.isPre())
			return;
				
		if(!registeredObjects.containsKey(event.getKey()))
			return;
		
		try {
			Object rObject = registeredObjects.get(event.getKey());
			ByteArrayInputStream bais = new ByteArrayInputStream(( (ByteBuffer)event.getValue()).array());
			ObjectInputStream ois = new ObjectInputStream(bais);
			int callId = ois.readInt();
			String method = (String)ois.readObject();
			
			Object ret = null;
			Object[] args = (Object[])ois.readObject();			
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
	            	break;
	            }
	        }	        
	        
	        if(ret == null)
	        	throw new IllegalStateException("Method not found.");
	        
			if(registeredCalls.containsKey(callId)){
				registeredCalls.get(callId).setReturnValue(ret);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
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
