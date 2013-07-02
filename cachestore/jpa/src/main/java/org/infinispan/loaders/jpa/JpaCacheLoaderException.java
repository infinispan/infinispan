package org.infinispan.loaders.jpa;

import org.infinispan.loaders.CacheLoaderException;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class JpaCacheLoaderException extends CacheLoaderException {

	private static final long serialVersionUID = -5941891649874210344L;

	public JpaCacheLoaderException() {
		super();
	}

	public JpaCacheLoaderException(String message, Throwable cause) {
		super(message, cause);
	}

	public JpaCacheLoaderException(String message) {
		super(message);
	}

	public JpaCacheLoaderException(Throwable cause) {
		super(cause);
	}

}
