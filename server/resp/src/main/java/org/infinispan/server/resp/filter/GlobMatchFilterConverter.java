package org.infinispan.server.resp.filter;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.GlobUtils;
import org.infinispan.commons.util.Util;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

/**
 * A {@link org.infinispan.filter.KeyValueFilterConverter} which matches the key against a glob pattern. Since the
 * value is unused, it just returns an empty byte array
 *
 * @since 15.0
 **/
@ProtoName("GlobMatchFilterConverter")
public class GlobMatchFilterConverter<K, V> extends AbstractKeyValueFilterConverter<byte[], V, byte[]> {
   @ProtoField()
   final String glob;

   @ProtoField(number = 2, defaultValue = "false")
   final boolean returnValue;
   private transient final Pattern pattern;

   @ProtoFactory
   GlobMatchFilterConverter(String glob, boolean returnValue) {
      this.glob = glob;
      this.returnValue = returnValue;
      this.pattern = Pattern.compile(GlobUtils.globToRegex(glob));
   }

   @Override
   public MediaType format() {
      return null;
   }

   @Override
   public byte[] filterAndConvert(byte[] key, V value, Metadata metadata) {
      String k = new String(key, StandardCharsets.UTF_8);
      if (pattern.matcher(k).matches()) {
         return returnValue ? (byte[]) value : Util.EMPTY_BYTE_ARRAY;
      } else {
         return null;
      }
   }
}
