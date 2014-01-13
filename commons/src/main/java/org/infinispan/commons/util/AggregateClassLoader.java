package org.infinispan.commons.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * @author Brett Meyer
 */
public class AggregateClassLoader extends ClassLoader {

	public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class[0];

	private static final Log LOG = LogFactory.getLog( AggregateClassLoader.class );

	private static final Class<?>[] primitives = { int.class, byte.class, short.class, long.class, float.class, double.class, boolean.class, char.class };

	private static final Class<?>[] primitiveArrays = { int[].class, byte[].class, short[].class, long[].class, float[].class, double[].class, boolean[].class,
			char[].class };

	private final ClassLoader osgiClassLoader = new OsgiClassLoader();

	private WeakReference<ClassLoader> configurationClassLoader = null;
	
	public AggregateClassLoader() {
		// DO NOT use ClassLoader#parent, which is typically the SystemClassLoader for most containers. Instead,
		// allow the ClassNotFoundException to be thrown. ClassLoaderServiceImpl will check the SystemClassLoader
		// later on. This is especially important for embedded OSGi containers, etc.
		super( null );
	}

	public void setConfigurationClassLoader(ClassLoader configurationClassLoader) {
		this.configurationClassLoader = new WeakReference<ClassLoader>( configurationClassLoader );
	}

	/**
	 * Load the class and break on first found match. 
	 */
	@Override
	@SuppressWarnings("rawtypes")
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		for ( ClassLoader classLoader : getClassLoaders() ) {
			try {
				final Class clazz = classLoader.loadClass( name );
				if ( clazz != null ) {
					return clazz;
				}
			}
			catch (Exception ignore) {
			}
		}

		throw new ClassNotFoundException( "Could not load requested class : " + name );
	}

	/**
	 * Load the resource and break on first found match.
	 */
	@Override
	protected URL findResource(String name) {
		for ( ClassLoader classLoader : getClassLoaders() ) {
			try {
				final URL resource = classLoader.getResource( name );
				if ( resource != null ) {
					return resource;
				}
			}
			catch (Exception ignore) {
			}
		}

		// TODO: Error?
		return null;
	}

	/**
	 * Load the resources and return an Enumeration
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected Enumeration<URL> findResources(String name) {
		final List<Enumeration<URL>> enumerations = new ArrayList<Enumeration<URL>>();

		for ( ClassLoader classLoader : getClassLoaders() ) {
			try {
				final Enumeration<URL> resources = classLoader.getResources( name );
				if ( resources != null ) {
					enumerations.add( resources );
				}
			}
			catch (Exception ignore) {
			}
		}

		final Enumeration<URL> aggEnumeration = new Enumeration<URL>() {

			@Override
			public boolean hasMoreElements() {
				for ( Enumeration<URL> enumeration : enumerations ) {
					if ( enumeration != null && enumeration.hasMoreElements() ) {
						return true;
					}
				}
				return false;
			}

			@Override
			public URL nextElement() {
				for ( Enumeration<URL> enumeration : enumerations ) {
					if ( enumeration != null && enumeration.hasMoreElements() ) {
						return enumeration.nextElement();
					}
				}
				throw new NoSuchElementException();
			}
		};

		return aggEnumeration;
	}

	/**
	 * <p>
	 * Loads the specified class using the passed classloader, or, if it is <code>null</code> the Infinispan classes'
	 * classloader.
	 * </p>
	 * <p>
	 * If loadtime instrumentation via GenerateInstrumentedClassLoader is used, this class may be loaded by the
	 * bootstrap classloader.
	 * </p>
	 * <p>
	 * If the class is not found, the {@link ClassNotFoundException} or {@link NoClassDefFoundError} is wrapped as a
	 * {@link CacheConfigurationException} and is re-thrown.
	 * </p>
	 * 
	 * @param classname name of the class to load
	 * @param cl the application classloader which should be used to load the class, or null if the class is always
	 * packaged with Infinispan
	 * @return the class
	 * @throws CacheConfigurationException if the class cannot be loaded
	 */
	public <T> Class<T> loadClass(String classname, ClassLoader cl) {
		try {
			return loadClassStrict( classname, cl );
		}
		catch (ClassNotFoundException e) {
			throw new CacheConfigurationException( "Unable to instantiate class " + classname, e );
		}
	}

	/**
	 * <p>
	 * Loads the specified class using the passed classloader, or, if it is <code>null</code> the Infinispan classes'
	 * classloader.
	 * </p>
	 * <p>
	 * If loadtime instrumentation via GenerateInstrumentedClassLoader is used, this class may be loaded by the
	 * bootstrap classloader.
	 * </p>
	 * 
	 * @param classname name of the class to load
	 * @return the class
	 * @param userClassLoader the application classloader which should be used to load the class, or null if the class
	 * is always packaged with Infinispan
	 * @throws ClassNotFoundException if the class cannot be loaded
	 */
	@SuppressWarnings("unchecked")
	public <T> Class<T> loadClassStrict(String classname, ClassLoader userClassLoader) throws ClassNotFoundException {
		List<ClassLoader> cls = getClassLoaders( userClassLoader );
		ClassNotFoundException e = null;
		NoClassDefFoundError ne = null;
		for ( ClassLoader cl : cls ) {
			if ( cl == null )
				continue;

			try {
				return (Class<T>) Class.forName( classname, true, cl );
			}
			catch (ClassNotFoundException ce) {
				e = ce;
			}
			catch (NoClassDefFoundError ce) {
				ne = ce;
			}
		}

		if ( e != null )
			throw e;
		else if ( ne != null ) {
			// Before we wrap this, make sure we appropriately log this.
			LOG.unableToLoadClass( classname, Arrays.toString( cls.toArray() ), ne );
			throw new ClassNotFoundException( classname, ne );
		}
		else
			throw new IllegalStateException();
	}

	/**
	 * <p>
	 * Loads the specified class using the passed classloader, or, if it is <code>null</code> the Infinispan classes'
	 * classloader.
	 * </p>
	 * <p>
	 * If loadtime instrumentation via GenerateInstrumentedClassLoader is used, this class may be loaded by the
	 * bootstrap classloader.
	 * </p>
	 * 
	 * @param classname name of the class to load
	 * @return the class
	 * @param userClassLoader the application classloader which should be used to load the class, or null if the class
	 * is always packaged with Infinispan
	 * @throws ClassNotFoundException if the class cannot be loaded
	 */
	@SuppressWarnings("unchecked")
	public <T> Class<T> loadClassStrict(String classname) throws ClassNotFoundException {
		return loadClassStrict(classname, null);
	}

	/**
	 * Instantiates a class based on the class name provided. Instantiation is attempted via an appropriate, static
	 * factory method named <tt>getInstance()</tt> first, and failing the existence of an appropriate factory, falls
	 * back to an empty constructor.
	 * <p />
	 * Any exceptions encountered loading and instantiating the class is wrapped in a
	 * {@link CacheConfigurationException}.
	 * 
	 * @param classname class to instantiate
	 * @return an instance of classname
	 */
	public <T> T getInstance(String classname, ClassLoader cl) {
		if ( classname == null )
			throw new IllegalArgumentException( "Cannot load null class!" );
		Class<T> clazz = loadClass( classname, cl );
		return getInstance( clazz );
	}

	/**
	 * Similar to {@link #getInstance(String, ClassLoader)} except that exceptions are propagated to the caller.
	 * 
	 * @param classname class to instantiate
	 * @return an instance of classname
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public <T> T getInstanceStrict(String classname, ClassLoader cl) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		if ( classname == null )
			throw new IllegalArgumentException( "Cannot load null class!" );
		Class<T> clazz = loadClassStrict( classname, cl );
		return getInstanceStrict( clazz );
	}

	/**
	 * Instantiates a class by first attempting a static <i>factory method</i> named <tt>getInstance()</tt> on the class
	 * and then falling back to an empty constructor.
	 * <p/>
	 * Any exceptions encountered are wrapped in a {@link CacheConfigurationException} and rethrown.
	 * 
	 * @param clazz class to instantiate
	 * @return an instance of the class
	 */
	public <T> T getInstance(Class<T> clazz) {
		try {
			return getInstanceStrict( clazz );
		}
		catch (IllegalAccessException iae) {
			throw new CacheConfigurationException( "Unable to instantiate class " + clazz.getName(), iae );
		}
		catch (InstantiationException ie) {
			throw new CacheConfigurationException( "Unable to instantiate class " + clazz.getName(), ie );
		}
	}

	/**
	 * Instantiates a class based on the class name provided. Instantiation is attempted via an appropriate, static
	 * factory method named <tt>getInstance()</tt> first, and failing the existence of an appropriate factory, falls
	 * back to an empty constructor.
	 * <p />
	 * Any exceptions encountered loading and instantiating the class is wrapped in a
	 * {@link CacheConfigurationException}.
	 * 
	 * @param classname class to instantiate
	 * @return an instance of classname
	 */
	public <T> T getInstance(String classname) {
		if ( classname == null )
			throw new IllegalArgumentException( "Cannot load null class!" );
		try {
			Class<T> clazz = (Class<T>) loadClass( classname );
			return getInstance( clazz );
		}
		catch (ClassNotFoundException e) {
			throw new CacheConfigurationException( "Unable to instantiate class " + classname, e );
		}
	}

	/**
	 * Similar to {@link #getInstance(Class)} except that exceptions are propagated to the caller.
	 * 
	 * @param clazz class to instantiate
	 * @return an instance of the class
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	@SuppressWarnings("unchecked")
	public <T> T getInstanceStrict(Class<T> clazz) throws IllegalAccessException, InstantiationException {
		// first look for a getInstance() constructor
		T instance = null;
		try {
			Method factoryMethod = getFactoryMethod( clazz );
			if ( factoryMethod != null )
				instance = (T) factoryMethod.invoke( null );
		}
		catch (Exception e) {
			// no factory method or factory method failed. Try a constructor.
			instance = null;
		}
		if ( instance == null ) {
			instance = clazz.newInstance();
		}
		return instance;
	}

	/**
	 * Looks up the file, see : {@link DefaultFileLookup}.
	 * 
	 * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
	 * @return an input stream to the file or null if nothing found through all lookup steps.
	 */
	public InputStream lookupFile(String filename, ClassLoader cl) {
		InputStream is = filename == null || filename.length() == 0 ? null : getAsInputStreamFromClassLoader( filename, cl );
		if ( is == null ) {
			if ( LOG.isDebugEnabled() )
				LOG.debugf( "Unable to find file %s in classpath; searching for this file on the filesystem instead.", filename );
			try {
				is = new FileInputStream( filename );
			}
			catch (FileNotFoundException e) {
				return null;
			}
		}
		return is;
	}

	/**
	 * Looks up the file, see : {@link DefaultFileLookup}.
	 * 
	 * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
	 * @return an input stream to the file or null if nothing found through all lookup steps.
	 * @throws FileNotFoundException if file cannot be found
	 */
	public InputStream lookupFileStrict(String filename, ClassLoader cl) throws FileNotFoundException {
		InputStream is = filename == null || filename.length() == 0 ? null : getAsInputStreamFromClassLoader( filename, cl );
		if ( is == null ) {
			if ( LOG.isDebugEnabled() )
				LOG.debugf( "Unable to find file %s in classpath; searching for this file on the filesystem instead.", filename );
			return new FileInputStream( filename );
		}
		return is;
	}
	
	public InputStream lookupFileStrict(String filename) throws FileNotFoundException {
		return lookupFileStrict(filename, null);
	}

	public InputStream lookupFileStrict(URI uri, ClassLoader cl) throws FileNotFoundException {
		return new FileInputStream( new File( uri ) );
	}

	public URL lookupFileLocation(String filename, ClassLoader cl) {
		URL u = getAsURLFromClassLoader( filename, cl );

		if ( u == null ) {
			File f = new File( filename );
			if ( f.exists() )
				try {
					u = f.toURI().toURL();
				}
				catch (MalformedURLException e) {
					// what do we do here?
				}
		}
		return u;
	}

	public URL lookupFileLocation(String filename) {
		return lookupFileLocation(filename, null);
	}

	public Collection<URL> lookupFileLocations(String filename, ClassLoader cl) throws IOException {
		Collection<URL> u = getAsURLsFromClassLoader( filename, cl );

		File f = new File( filename );
		if ( f.exists() )
			try {
				u.add( f.toURI().toURL() );
			}
			catch (MalformedURLException e) {
				// what do we do here?
			}
		return u;
	}

	public Class<?>[] toClassArray(String[] typeList, ClassLoader classLoader) throws ClassNotFoundException {
		if ( typeList == null )
			return EMPTY_CLASS_ARRAY;
		Class<?>[] retval = new Class[typeList.length];
		int i = 0;
		for ( String s : typeList )
			retval[i++] = getClassForName( s, classLoader );
		return retval;
	}

	public Class<?>[] toClassArray(String[] typeList) throws ClassNotFoundException {
		return toClassArray(typeList, null);
	}

	public Class<?> getClassForName(String name, ClassLoader cl) throws ClassNotFoundException {
		try {
			return loadClassStrict( name, cl );
		}
		catch (ClassNotFoundException cnfe) {
			// Could be a primitive - let's check
			for ( Class<?> primitive : primitives )
				if ( name.equals( primitive.getName() ) )
					return primitive;
			for ( Class<?> primitive : primitiveArrays )
				if ( name.equals( primitive.getName() ) )
					return primitive;
		}
		throw new ClassNotFoundException( "Class " + name + " cannot be found" );
	}

	private InputStream getAsInputStreamFromClassLoader(String filename, ClassLoader appClassLoader) {
		for ( ClassLoader cl : getClassLoaders( appClassLoader ) ) {
			if ( cl == null )
				continue;
			try {
				return cl.getResourceAsStream( filename );
			}
			catch (RuntimeException e) {
				// Ignore this as the classloader may throw exceptions for a valid path on Windows
			}
		}
		return null;
	}

	private URL getAsURLFromClassLoader(String filename, ClassLoader userClassLoader) {
		for ( ClassLoader cl : getClassLoaders( userClassLoader ) ) {
			if ( cl == null )
				continue;

			try {
				return cl.getResource( filename );
			}
			catch (RuntimeException e) {
				// Ignore this as the classloader may throw exceptions for a valid path on Windows
			}
		}
		return null;
	}

	private Collection<URL> getAsURLsFromClassLoader(String filename, ClassLoader userClassLoader) throws IOException {
		Collection<URL> urls = new HashSet<URL>( 4 );
		for ( ClassLoader cl : getClassLoaders( userClassLoader ) ) {
			if ( cl == null )
				continue;
			try {
				urls.addAll( new EnumerationList<URL>( cl.getResources( filename ) ) );
			}
			catch (RuntimeException e) {
				// Ignore this as the classloader may throw exceptions for a valid path on Windows
			}
		}
		return urls;
	}

	private Method getFactoryMethod(Class<?> c) {
		for ( Method m : c.getMethods() ) {
			if ( m.getName().equals( "getInstance" ) && m.getParameterTypes().length == 0 && Modifier.isStatic( m.getModifiers() ) )
				return m;
		}
		return null;
	}

	// TODO: Initialize once?
	private List<ClassLoader> getClassLoaders(ClassLoader userClassLoader) {
		final List<ClassLoader> prioritizedClassLoaders = new ArrayList<ClassLoader>();
		// must attempt OSGi first
		prioritizedClassLoaders.add( osgiClassLoader );
		if ( userClassLoader != null ) {
			prioritizedClassLoaders.add( userClassLoader );
		}
		if ( configurationClassLoader != null ) {
			prioritizedClassLoaders.add( configurationClassLoader.get() );
		}
		// Infinispan classes (not always on TCCL [modular env])
		prioritizedClassLoaders.add( AggregateClassLoader.class.getClassLoader() ); 
		// Used when load time instrumentation is in effect
		prioritizedClassLoaders.add( ClassLoader.getSystemClassLoader() ); 
		// final fallback is TCCL
		prioritizedClassLoaders.add( Thread.currentThread().getContextClassLoader() );
		return prioritizedClassLoaders;
	}
	private List<ClassLoader> getClassLoaders() {
		final List<ClassLoader> prioritizedClassLoaders = new ArrayList<ClassLoader>();
		// must attempt OSGi first
		prioritizedClassLoaders.add( osgiClassLoader );
		if ( configurationClassLoader != null ) {
			prioritizedClassLoaders.add( configurationClassLoader.get() );
		}
		// Infinispan classes (not always on TCCL [modular env])
		prioritizedClassLoaders.add( AggregateClassLoader.class.getClassLoader() ); 
		// Used when load time instrumentation is in effect
		prioritizedClassLoaders.add( ClassLoader.getSystemClassLoader() ); 
		// final fallback is TCCL
		prioritizedClassLoaders.add( Thread.currentThread().getContextClassLoader() );
		return prioritizedClassLoaders;
	}
}
