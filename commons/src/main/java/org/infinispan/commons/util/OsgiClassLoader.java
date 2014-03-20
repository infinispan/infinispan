package org.infinispan.commons.util;

import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleReference;

/**
 * @author Brett Meyer
 */
public class OsgiClassLoader extends ClassLoader {

	// TODO: Eventually, it would be better to limit this in scope to *only* what's needed, rather than all bundles
	// in the container.
	private final Bundle[] bundles;

	private final Map<String, Class<?>> classCache = new HashMap<String, Class<?>>();
	private final Map<String, URL> resourceCache = new HashMap<String, URL>();

	protected OsgiClassLoader() {
		// DO NOT use ClassLoader#parent, which is typically the SystemClassLoader for most containers. Instead,
		// allow the ClassNotFoundException to be thrown. ClassLoaderServiceImpl will check the SystemClassLoader
		// later on. This is especially important for embedded OSGi containers, etc.
		super( null );
		
		// TODO: move this to core and include core CL?
		final ClassLoader cl = OsgiClassLoader.class.getClassLoader();
		if (cl instanceof BundleReference) {
			final BundleContext bundleContext = ( (BundleReference) OsgiClassLoader.class.getClassLoader() ).getBundle()
					.getBundleContext();
			bundles = bundleContext.getBundles();
		} else {
			bundles = new Bundle[0];
		}
	}

	/**
	 * Load the class and break on first found match.  
	 * 
	 * TODO: Should this throw a different exception or warn if multiple
	 * classes were found? Naming collisions can and do happen in OSGi...
	 */
	@Override
	@SuppressWarnings("rawtypes")
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		if ( classCache.containsKey( name ) ) {
			return classCache.get( name );
		}

		for ( Bundle bundle : bundles ) {
			try {
				final Class clazz = bundle.loadClass( name );
				if ( clazz != null ) {
					classCache.put( name, clazz );
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
	 * 
	 * TODO: Should this throw a different exception or warn if multiple
	 * resources were found? Naming collisions can and do happen in OSGi...
	 */
	@Override
	protected URL findResource(String name) {
		if ( resourceCache.containsKey( name ) ) {
			return resourceCache.get( name );
		}

		for ( Bundle bundle : bundles ) {
			try {
				final URL resource = bundle.getResource( name );
				if ( resource != null ) {
					resourceCache.put( name, resource );
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
	 * 
	 * Note: Since they're Enumerations, do not cache these results!
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected Enumeration<URL> findResources(String name) {
		final List<Enumeration<URL>> enumerations = new ArrayList<Enumeration<URL>>();

		for ( Bundle bundle : bundles ) {
			try {
				final Enumeration<URL> resources = bundle.getResources( name );
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

}
