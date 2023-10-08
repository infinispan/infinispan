package org.infinispan.server.resp.filter;

import java.nio.charset.StandardCharsets;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.GlobMatcher;
import org.infinispan.commons.util.Util;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A {@link org.infinispan.filter.KeyValueFilterConverter} which matches the key against a glob pattern. Since the
 * value is unused, it just returns an empty byte array
 *
 * @since 15.0
 **/
@ProtoTypeId(ProtoStreamTypeIds.RESP_GLOB_MATCH_FILTER_CONVERTER)
public class GlobMatchFilterConverter<K, V> extends AbstractKeyValueFilterConverter<byte[], V, byte[]> {
   @ProtoField(number = 1)
   final String glob;

   @ProtoField(number = 2, defaultValue = "false")
   final boolean returnValue;
   private transient final byte[] pattern;

   @ProtoFactory
   GlobMatchFilterConverter(String glob, boolean returnValue) {
      this.glob = glob;
      this.returnValue = returnValue;
      this.pattern = glob.getBytes(StandardCharsets.US_ASCII);
   }

   @Override
   public MediaType format() {
      return null;
   }

   @Override
   public byte[] filterAndConvert(byte[] key, V value, Metadata metadata) {
      if (GlobMatcher.match(pattern, key)) {
         return returnValue ? (byte[]) value : Util.EMPTY_BYTE_ARRAY;
      } else {
         return null;
      }
   }
}
