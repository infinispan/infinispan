package org.infinispan.io;


import java.io.OutputStream;

import org.jboss.marshalling.ByteOutput;

/**
 * A stream of bytes which can be written to, and the underlying byte array can be directly accessed.
 *
 * By implementing {@link org.jboss.marshalling.ByteOutput} we avoid the need for the byte stream to be wrapped by
 * {@link org.jboss.marshalling.Marshalling#createByteOutput(OutputStream)}
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @since 5.1
 */
public abstract class MarshalledValueByteStream extends OutputStream implements ByteOutput {

   public abstract int size();

   public abstract byte[] getRaw();

}
