package org.infinispan.dataconversion;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;

public class GzipEncoder implements Encoder {

   @Override
   public Object toStorage(Object content) {
      assert content instanceof String;
      return compress(content.toString());
   }

   @Override
   public Object fromStorage(Object content) {
      assert content instanceof byte[];
      return decompress((byte[]) content);
   }

   private byte[] compress(String str) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
           GZIPOutputStream gis = new GZIPOutputStream(baos)) {
         gis.write(str.getBytes("UTF-8"));
         gis.close();
         return baos.toByteArray();
      } catch (IOException e) {
         throw new RuntimeException("Unable to compress", e);
      }
   }

   private String decompress(byte[] compressed) {
      try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
           BufferedReader bf = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8))) {
         StringBuilder result = new StringBuilder();
         String line;
         while ((line = bf.readLine()) != null) {
            result.append(line);
         }
         return result.toString();
      } catch (IOException e) {
         throw new RuntimeException("Unable to decompress", e);
      }
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.parse("application/gzip");
   }

   @Override
   public boolean isStorageFormatFilterable() {
      return false;
   }

   @Override
   public short id() {
      return 10000;
   }
}
