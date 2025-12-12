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
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.infinispan.commons.util.Util;

public class Utils {
   public static final int BUFFER_SIZE = 8192;

   public static String sha256(Path path) {
      return digest(path, "SHA-256");
   }

   public static String digest(Path path, String algorithm) {
      try (ByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ)) {
         MessageDigest digest = MessageDigest.getInstance(algorithm);
         if (channel instanceof FileChannel fileChannel) {
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

   private enum CharacterClass {
      ALNUM("[:alnum:]", "\\p{Alnum}"),
      ALPHA("[:alpha:]", "\\p{Alpha}"),
      DIGIT("[:digit:]", "\\p{Digit}"),
      GRAPH("[:graph:]", "\\p{Graph}"),
      LOWER("[:lower:]", "\\p{Lower}"),
      UPPER("[:upper:]", "\\p{Upper}"),
      PUNCT("[:punct:]", "\\p{Punct}"),
      XDIGIT("[:xdigit:]", "\\p{XDigit}");

      private final String cclass;
      private final Pattern pattern;

      private static final Map<String, CharacterClass> classes;
      private static final String ALL = IntStream.rangeClosed(33, 126)
         .collect(StringBuilder::new,
            StringBuilder::appendCodePoint,
            StringBuilder::append)
         .toString();

      static {
         final Map<String, CharacterClass> map = new HashMap<>(64);
         for (CharacterClass cClass : values()) {
            map.put(cClass.cclass, cClass);
         }
         classes = map;
      }

      public static CharacterClass of(String cclass) {
         return classes.get(cclass);
      }

      CharacterClass(String cclass, String regex) {
         this.cclass = cclass;
         this.pattern = Pattern.compile(regex);
      }
   }

   public static String randomString(String charClass, int length) {
      CharacterClass characterClass = CharacterClass.of(charClass);
      if (characterClass == null) {
         throw new IllegalArgumentException("Unknown character class " + charClass);
      }
      StringBuilder sb = new StringBuilder();
      SecureRandom random = new SecureRandom();
      while (sb.length() < length) {
         int i = random.nextInt(CharacterClass.ALL.length());
         char ch = CharacterClass.ALL.charAt(i);
         if (characterClass.pattern.matcher(String.valueOf(ch)).matches()) {
            sb.append(ch);
         }
      }
      return sb.toString();
   }
}
