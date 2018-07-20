/**
 * This package should mainly contain {@link org.infinispan.protostream.MessageMarshaller} implementations for classes
 * which a static inner class is not possible. For example it's necessary for the {@link org.infinispan.commons.marshall.WrappedByteArray}
 * marshaller to be in this package as the infinispan-commons module does not contain a dependency on protostream.
 *
 * @author Ryan Emerson
 * @since 10.0
 * @private
 */
package org.infinispan.marshall.persistence.impl;
