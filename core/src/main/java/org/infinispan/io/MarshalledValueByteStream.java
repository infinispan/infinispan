package org.infinispan.io;


import java.io.OutputStream;

/**
 * A stream of bytes which can be written to, and the underlying byte array can be directly accessed.
 *
 * If you require a {@link org.jboss.marshalling.ByteOutput} instance, then you now need to wrap the stream via
 * {@link org.jboss.marshalling.Marshalling#createByteOutput(OutputStream)}
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @since 5.1
 * @deprecated since 10.0 requires jboss-marshalling-osgi artifact to be provided at runtime.
 */
@Deprecated
public abstract class MarshalledValueByteStream extends OutputStream {

   public abstract int size();

   public abstract byte[] getRaw();

}
