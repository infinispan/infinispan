package org.infinispan.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

public interface Compression {
   byte[] compress(String str);

   String decompress(byte[] compressed);

   String name();

   Compression GZIP = new Compression() {
      @Override
      public byte[] compress(String str) {
         try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
              GZIPOutputStream os = new GZIPOutputStream(baos)) {
            os.write(str.getBytes(UTF_8));
            os.close();
            return baos.toByteArray();
         } catch (IOException e) {
            throw new RuntimeException("Unable to compress", e);
         }
      }

      @Override
      public String decompress(byte[] compressed) {
         try (GZIPInputStream is = new GZIPInputStream(new ByteArrayInputStream(compressed));
              ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            is.transferTo(os);
            return os.toString(UTF_8);
         } catch (IOException e) {
            throw new RuntimeException("Unable to decompress", e);
         }
      }

      @Override
      public String name() {
         return "gzip";
      }
   };

   Compression DEFLATE = new Compression() {
      @Override
      public byte[] compress(String str) {
         try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
              DeflaterOutputStream os = new DeflaterOutputStream(baos)) {
            os.write(str.getBytes(UTF_8));
            os.close();
            return baos.toByteArray();
         } catch (IOException e) {
            throw new RuntimeException("Unable to compress", e);
         }
      }

      @Override
      public String decompress(byte[] compressed) {
         try (InflaterInputStream is = new InflaterInputStream(new ByteArrayInputStream(compressed));
              ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            is.transferTo(os);
            return os.toString(UTF_8);
         } catch (IOException e) {
            throw new RuntimeException("Unable to decompress", e);
         }
      }

      @Override
      public String name() {
         return "deflate";
      }
   };

}
