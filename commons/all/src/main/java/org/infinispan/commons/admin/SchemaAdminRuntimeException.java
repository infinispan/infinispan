package org.infinispan.commons.admin;

/**
 * Exception raised when managing schemas.
 * Might wrap another exception such as {@link org.infinispan.commons.CacheException}
 * @since 16.0
 */
public class SchemaAdminRuntimeException extends RuntimeException {

    public SchemaAdminRuntimeException(Throwable ex) {
        super(ex);
    }

    public SchemaAdminRuntimeException(String message) {
        super(message);
    }
}
