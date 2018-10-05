package org.infinispan.client.hotrod.impl.protocol;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.MediaTypeIds;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * @author gustavonalle
 * @since 8.0
 */
public final class CodecUtils {

   private CodecUtils() {
   }

   static boolean isGreaterThan4bytes(long value) {
      int narrowed = (int) value;
      return narrowed == value;
   }

   public static int toSeconds(long duration, TimeUnit timeUnit) {
      int seconds = (int) timeUnit.toSeconds(duration);
      long inverseDuration = timeUnit.convert(seconds, TimeUnit.SECONDS);

      if (duration > inverseDuration) {
         //Round up.
         seconds++;
      }
      return seconds;
   }

   static MediaType readMediaType(ByteBuf byteBuf) {
      byte keyMediaTypeDefinition = byteBuf.readByte();
      if (keyMediaTypeDefinition == 0) return null;
      if (keyMediaTypeDefinition == 1) return readPredefinedMediaType(byteBuf);
      if (keyMediaTypeDefinition == 2) return readCustomMediaType(byteBuf);
      throw new HotRodClientException("Unknown MediaType definition");
   }

   static MediaType readPredefinedMediaType(ByteBuf buffer) {
      int mediaTypeId = ByteBufUtil.readVInt(buffer);
      MediaType mediaType = MediaTypeIds.getMediaType((short) mediaTypeId);
      return mediaType.withParameters(readMediaTypeParams(buffer));
   }

   static MediaType readCustomMediaType(ByteBuf buffer) {
      byte[] customMediaTypeBytes = ByteBufUtil.readArray(buffer);
      String strCustomMediaType = new String(customMediaTypeBytes, CharsetUtil.UTF_8);
      MediaType customMediaType = MediaType.parse(strCustomMediaType);
      return customMediaType.withParameters(readMediaTypeParams(buffer));
   }

   static Map<String, String> readMediaTypeParams(ByteBuf buffer) {
      int paramsSize = ByteBufUtil.readVInt(buffer);
      if (paramsSize == 0) return Collections.emptyMap();
      Map<String, String> params = new HashMap<>(paramsSize);
      for (int i = 0; i < paramsSize; i++) {
         byte[] bytesParamName = ByteBufUtil.readArray(buffer);
         String paramName = new String(bytesParamName, CharsetUtil.UTF_8);
         byte[] bytesParamValue = ByteBufUtil.readArray(buffer);
         String paramValue = new String(bytesParamValue, CharsetUtil.UTF_8);
         params.put(paramName, paramValue);
      }
      return params;
   }

}
