package org.infinispan.cli.util;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

import org.infinispan.commons.util.Util;

public class Utils {
   public static final int BUFFER_SIZE = 8192;

   public static String sha256(Path path) {
      return digest(path, "SHA-256");
   }

   public static String digest(Path path, String algorithm) {
      try (ByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ)) {
         MessageDigest digest = MessageDigest.getInstance(algorithm);
         if (channel instanceof FileChannel) {
            FileChannel fileChannel = (FileChannel) channel;
            MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            digest.update(byteBuffer);
         } else {
            ByteBuffer bb = ByteBuffer.allocate(BUFFER_SIZE);
            while (channel.read(bb) != -1) {
               bb.flip();
               digest.update(bb);
               bb.flip();
            }
         }
         return Util.toHexString(digest.digest());
      } catch (NoSuchFileException e) {
         return null;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
