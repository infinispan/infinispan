package org.infinispan.rest.framework;

/**
 * @since 10.0
 */
public interface ContentSource {

   String asString();

   byte[] rawContent();

}
