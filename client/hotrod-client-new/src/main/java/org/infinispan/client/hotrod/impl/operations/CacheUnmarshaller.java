package org.infinispan.client.hotrod.impl.operations;

import io.netty.buffer.ByteBuf;

public interface CacheUnmarshaller {
   /**
    * Reads a variable int array first and then reads a key using the value media type only using up to the
    * maximum provided
    * number of bytes
    * @param buf
    * @return
    * @param <E>
    */
   <E> E readKey(ByteBuf buf);

   /**
    * Reads a key from the provided ByteBuf using the value media type up to the maximum number of bytes.
    * @param buf
    * @param maxLength
    * @return
    * @param <E>
    */
   <E> E readKey(ByteBuf buf, int maxLength);

   /**
    * Reads a variable int array first and then reads a value using the value media type only using up to the
    * maximum provided
    * number of bytes
    * @param buf
    * @return
    * @param <E>
    */
   <E> E readValue(ByteBuf buf);

   /**
    * Reads a value from the provided ByteBuf using the value media type up to the maximum number of bytes.
    * @param buf
    * @param maxLength
    * @return
    * @param <E>
    */
   <E> E readValue(ByteBuf buf, int maxLength);

   /**
    * Reads a variable int first and then reads an arbitrary object with the configured Marshaller up to the
    * maximum number of bytes.
    * @param buf
    * @return
    * @param <E>
    */
   <E> E readOther(ByteBuf buf);

   /**
    * Reads an arbitrary object with the configured Marshaller up to the maximum number of bytes.
    * @param buf
    * @param maxLength
    * @return
    * @param <E>
    */
   <E> E readOther(ByteBuf buf, int maxLength);
}
