package org.infinispan.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class Gzip {

   public static byte[] compress(String str) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
           GZIPOutputStream gis = new GZIPOutputStream(baos)) {
         gis.write(str.getBytes(UTF_8));
         gis.close();
         return baos.toByteArray();
      } catch (IOException e) {
         throw new RuntimeException("Unable to compress", e);
      }
   }

   public static String decompress(byte[] compressed) {
      try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
           ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
         byte[] buffer = new byte[1024];
         int len;
         while ((len = gis.read(buffer)) > 0) {
            baos.write(buffer, 0, len);
         }
         return new String(baos.toByteArray(), UTF_8);
      } catch (IOException e) {
         throw new RuntimeException("Unable to decompress", e);
      }
   }
}
