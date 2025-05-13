package org.infinispan.commons.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.naming.Context;
import javax.security.auth.Subject;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;

/**
 * General utility methods used throughout the Infinispan code base.
 *
 * @author <a href="brian.stansberry@jboss.com">Brian Stansberry</a>
 * @author Galder Zamarreño
 * @since 4.0
 */
public final class Util {

   private static final boolean IS_ARRAYS_DEBUG = Boolean.getBoolean("infinispan.arrays.debug");
   private static final int COLLECTIONS_LIMIT = Integer.getInteger("infinispan.collections.limit", 8);
   public static final int HEX_DUMP_LIMIT = Integer.getInteger("infinispan.hexdump.limit", 64);

   public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
   public static final String[] EMPTY_STRING_ARRAY = new String[0];
   public static final Throwable[] EMPTY_THROWABLE_ARRAY = new Throwable[0];
   public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
   public static final byte[][] EMPTY_BYTE_ARRAY_ARRAY = new byte[0][];
   public static final String GENERIC_JBOSS_MARSHALLING_CLASS = "org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller";
   public static final String JBOSS_USER_MARSHALLER_CLASS = "org.infinispan.jboss.marshalling.core.JBossUserMarshaller";

   private static final Log log = LogFactory.getLog(Util.class);

   private static final Set<Class<?>> BASIC_TYPES;

   private static final String HEX_VALUES = "0123456789ABCDEF";
   private static final char[] HEX_DUMP_CHARS = new char[256 * 2];

   static {
      BASIC_TYPES = new HashSet<>();
      BASIC_TYPES.add(Boolean.class);
      BASIC_TYPES.add(Byte.class);
      BASIC_TYPES.add(Character.class);
      BASIC_TYPES.add(Double.class);
      BASIC_TYPES.add(Float.class);
      BASIC_TYPES.add(Integer.class);
      BASIC_TYPES.add(Long.class);
      BASIC_TYPES.add(Short.class);
      BASIC_TYPES.add(String.class);
      BASIC_TYPES.add(char[].class);

      for (char b = 0; b < 256; b++) {
         if (0x20 <= b && b <= 0x7e) {
            HEX_DUMP_CHARS[b * 2] = '\\';
            HEX_DUMP_CHARS[b * 2 + 1] = b;
         } else {
            HEX_DUMP_CHARS[b * 2] = HEX_VALUES.charAt((b & 0xF0) >> 4);
            HEX_DUMP_CHARS[b * 2 + 1] = HEX_VALUES.charAt((b & 0x0F));
         }

      }
   }

   /**
    * <p>
    * Loads the specified class using the passed classloader, or, if it is <code>null</code> the Infinispan classes'
    * classloader.
    * </p>
    *
    * <p>
    * If loadtime instrumentation via GenerateInstrumentedClassLoader is used, this class may be loaded by the bootstrap
    * classloader.
    * </p>
    * <p>
    * If the class is not found, the {@link ClassNotFoundException} or {@link NoClassDefFoundError} is wrapped as a
    * {@link CacheConfigurationException} and is re-thrown.
    * </p>
    *
    * @param classname name of the class to load
    * @param cl        the application classloader which should be used to load the class, or null if the class is
    *                  always packaged with Infinispan
    * @return the class
    * @throws CacheConfigurationException if the class cannot be loaded
    */
   public static <T> Class<T> loadClass(String classname, ClassLoader cl) {
      try {
         return loadClassStrict(classname, cl);
      } catch (ClassNotFoundException e) {
         throw Log.CONTAINER.cannotInstantiateClass(classname, e);
      }
   }

   public static ClassLoader[] getClassLoaders(ClassLoader appClassLoader) {
      return new ClassLoader[]{
            appClassLoader,   // User defined classes
            Util.class.getClassLoader(),           // Infinispan classes (not always on TCCL [modular env])
            ClassLoader.getSystemClassLoader(),    // Used when load time instrumentation is in effect
            Thread.currentThread().getContextClassLoader() //Used by jboss-as stuff
      };
   }

   /**
    * <p>
    * Loads the specified class using the passed classloader, or, if it is <code>null</code> the Infinispan classes'
    * classloader.
    * </p>
    *
    * <p>
    * If loadtime instrumentation via GenerateInstrumentedClassLoader is used, this class may be loaded by the bootstrap
    * classloader.
    * </p>
    *
    * @param classname       name of the class to load
    * @param userClassLoader the application classloader which should be used to load the class, or null if the class is
    *                        always packaged with Infinispan
    * @return the class
    * @throws ClassNotFoundException if the class cannot be loaded
    */
   @SuppressWarnings("unchecked")
   public static <T> Class<T> loadClassStrict(String classname, ClassLoader userClassLoader) throws ClassNotFoundException {
      ClassLoader[] cls = getClassLoaders(userClassLoader);
      ClassNotFoundException e = null;
      NoClassDefFoundError ne = null;
      for (ClassLoader cl : cls) {
         if (cl == null)
            continue;

         try {
            return (Class<T>) Class.forName(classname, true, cl);
         } catch (ClassNotFoundException ce) {
            e = ce;
         } catch (NoClassDefFoundError ce) {
            ne = ce;
         }
      }
      if (ne != null) {
         //Always log the NoClassDefFoundError errors first:
         //if one happened they will contain critically useful details.
         log.unableToLoadClass(classname, Arrays.toString(cls), ne);
      }
      if (e != null)
         throw e;
      else if (ne != null) {
         throw new ClassNotFoundException(classname, ne);
      } else
         throw new IllegalStateException();
   }

   public static InputStream getResourceAsStream(String resourcePath, ClassLoader userClassLoader) {
      if (resourcePath.startsWith("/")) {
         resourcePath = resourcePath.substring(1);
      }
      InputStream is = null;
      for (ClassLoader cl : getClassLoaders(userClassLoader)) {
         if (cl != null) {
            is = cl.getResourceAsStream(resourcePath);
            if (is != null) {
               break;
            }
         }
      }
      return is;
   }

   public static String getResourceAsString(String resourcePath, ClassLoader userClassLoader) throws IOException {
      return read(getResourceAsStream(resourcePath, userClassLoader));
   }

   private static Method getFactoryMethod(Class<?> c) {
      for (Method m : c.getMethods()) {
         if (m.getName().equals("getInstance") && m.getParameterCount() == 0 && Modifier.isStatic(m.getModifiers()))
            return m;
      }
      return null;
   }

   /**
    * Instantiates a class by invoking the constructor that matches the provided parameter types passing the given
    * arguments. If no matching constructor is found this will return null. Note that the constructor must be public.
        * Any exceptions encountered are wrapped in a {@link CacheConfigurationException} and rethrown.
    *
    * @param clazz class to instantiate
    * @param <T>   the instance type
    * @return the new instance if a matching constructor was found otherwise null
    */
   public static <T> T newInstanceOrNull(Class<T> clazz, Class[] parameterTypes, Object... arguments) {
      if (parameterTypes.length != arguments.length) {
         throw new IllegalArgumentException("Parameter type count: " + parameterTypes.length +
               " does not match parameter arguments count: " + arguments.length);
      }
      try {
         Constructor<T> constructor = clazz.getDeclaredConstructor(parameterTypes);

         if (constructor != null) {
            return constructor.newInstance(arguments);
         }

      } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
         throw new CacheConfigurationException("Unable to instantiate class '" + clazz.getName() + "' with constructor " +
               "taking parameters " + Arrays.toString(arguments), e);
      }
      return null;
   }

   /**
    * Instantiates a class by first attempting a static <i>factory method</i> named <code>getInstance()</code> on the class
    * and then falling back to an empty constructor.
        * Any exceptions encountered are wrapped in a {@link CacheConfigurationException} and rethrown.
    *
    * @param clazz class to instantiate
    * @return an instance of the class
    */
   public static <T> T getInstance(Class<T> clazz) {
      try {
         return getInstanceStrict(clazz);
      } catch (IllegalAccessException | InstantiationException | NoSuchMethodException |
               InvocationTargetException iae) {
         throw new CacheConfigurationException("Unable to instantiate class '" + clazz.getName() + "'", iae);
      }
   }

   /**
    * Similar to {@link #getInstance(Class)} except that exceptions are propagated to the caller.
    *
    * @param clazz class to instantiate
    * @return an instance of the class
    * @throws IllegalAccessException
    * @throws InstantiationException
    */
   @SuppressWarnings("unchecked")
   public static <T> T getInstanceStrict(Class<T> clazz) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
      // first look for a getInstance() constructor
      T instance = null;
      try {
         Method factoryMethod = getFactoryMethod(clazz);
         if (factoryMethod != null) instance = (T) factoryMethod.invoke(null);
      } catch (Exception e) {
         // no factory method or factory method failed.  Try a constructor.
         instance = null;
      }
      if (instance == null) {
         instance = clazz.getDeclaredConstructor().newInstance();
      }
      return instance;
   }

   /**
    * Instantiates a class based on the class name provided.  Instantiation is attempted via an appropriate, static
    * factory method named <code>getInstance()</code> first, and failing the existence of an appropriate factory, falls back
    * to an empty constructor.
        * Any exceptions encountered loading and instantiating the class is wrapped in a
    * {@link CacheConfigurationException}.
    *
    * @param classname class to instantiate
    * @return an instance of classname
    */
   public static <T> T getInstance(String classname, ClassLoader cl) {
      if (classname == null) throw new IllegalArgumentException("Cannot load null class!");
      Class<T> clazz = loadClass(classname, cl);
      return getInstance(clazz);
   }

   /**
    * Similar to {@link #getInstance(String, ClassLoader)} except that exceptions are propagated to the caller.
    *
    * @param classname class to instantiate
    * @return an instance of classname
    * @throws ClassNotFoundException
    * @throws InstantiationException
    * @throws IllegalAccessException
    * @throws NoSuchMethodException
    * @throws InvocationTargetException
    */
   public static <T> T getInstanceStrict(String classname, ClassLoader cl) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
      if (classname == null) throw new IllegalArgumentException("Cannot load null class!");
      Class<T> clazz = loadClassStrict(classname, cl);
      return getInstanceStrict(clazz);
   }

   /**
    * Given two Runnables, return a Runnable that executes both in sequence, even if the first throws an exception, and
    * if both throw exceptions, add any exceptions thrown by the second as suppressed exceptions of the first.
    */
   public static Runnable composeWithExceptions(Runnable a, Runnable b) {
      return () -> {
         try {
            a.run();
         } catch (Throwable e1) {
            try {
               b.run();
            } catch (Throwable e2) {
               try {
                  e1.addSuppressed(e2);
               } catch (Throwable ignore) {
               }
            }
            throw e1;
         }
         b.run();
      };
   }


   /**
    * Prevent instantiation
    */
   private Util() {
   }

   public static String prettyPrintTime(long time, TimeUnit unit) {
      return prettyPrintTime(unit.toMillis(time));
   }

   /**
    * {@link System#nanoTime()} is less expensive than {@link System#currentTimeMillis()} and better suited to measure
    * time intervals. It's NOT suited to know the current time, for example to be compared with the time of other
    * nodes.
    *
    * @return the value of {@link System#nanoTime()}, but converted in Milliseconds.
    */
   public static long currentMillisFromNanotime() {
      return TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
   }

   /**
    * Prints a time for display
    *
    * @param millis time in millis
    * @return the time, represented as millis, seconds, minutes or hours as appropriate, with suffix
    */
   public static String prettyPrintTime(long millis) {
      if (millis < 1000) return millis + " milliseconds";
      NumberFormat nf = NumberFormat.getNumberInstance();
      nf.setMaximumFractionDigits(2);
      double toPrint = ((double) millis) / 1000;
      if (toPrint < 300) {
         return nf.format(toPrint) + " seconds";
      }

      toPrint = toPrint / 60;

      if (toPrint < 120) {
         return nf.format(toPrint) + " minutes";
      }

      toPrint = toPrint / 60;

      return nf.format(toPrint) + " hours";
   }

   /**
    * Reads the given InputStream fully, closes the stream and returns the result as a String.
    *
    * @param is the stream to read
    * @return the UTF-8 string
    * @throws java.io.IOException in case of stream read errors
    */
   public static String read(InputStream is) throws IOException {
      try (is) {
         final Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
         StringWriter writer = new StringWriter();
         char[] buf = new char[1024];
         int len;
         while ((len = reader.read(buf)) != -1) {
            writer.write(buf, 0, len);
         }
         return writer.toString();
      }
   }

   public static void close(AutoCloseable cl) {
      if (cl == null) return;
      try {
         cl.close();
      } catch (Exception e) {
         //Ignore
      }
   }

   public static void close(Socket s) {
      if (s == null) return;
      try {
         s.close();
      } catch (Exception e) {
      }
   }

   public static void close(AutoCloseable... cls) {
      for (AutoCloseable cl : cls) {
         close(cl);
      }
   }

   public static void close(Context ctx) {
      if (ctx == null) return;
      try {
         ctx.close();
      } catch (Exception e) {
      }
   }

   public static String formatString(Object message, Object... params) {
      if (params.length == 0) return message == null ? "null" : message.toString();

      return String.format(message.toString(), params);
   }

   public static String toStr(Object o) {
      if (o == null) {
         return "null";
      } else if (o.getClass().isArray()) {
         // as Java arrays are covariant, this cast is safe unless it's primitive
         if (o.getClass().getComponentType().isPrimitive()) {
            if (o instanceof byte[]) {
               return printArray((byte[]) o, false);
            } else if (o instanceof int[]) {
               return Arrays.toString((int[]) o);
            } else if (o instanceof long[]) {
               return Arrays.toString((long[]) o);
            } else if (o instanceof short[]) {
               return Arrays.toString((short[]) o);
            } else if (o instanceof double[]) {
               return Arrays.toString((double[]) o);
            } else if (o instanceof float[]) {
               return Arrays.toString((float[]) o);
            } else if (o instanceof char[]) {
               return Arrays.toString((char[]) o);
            } else if (o instanceof boolean[]) {
               return Arrays.toString((boolean[]) o);
            }
         }
         return Arrays.toString((Object[]) o);
      } else {
         return o.toString();
      }
   }

   public static <E> String toStr(Collection<E> collection) {
      if (collection == null)
         return "[]";

      Iterator<E> i = collection.iterator();
      if (!i.hasNext())
         return "[]";

      StringBuilder sb = new StringBuilder();
      sb.append('[');
      for (int counter = 0; ; ) {
         E e = i.next();
         sb.append(e == collection ? "(this Collection)" : toStr(e));
         if (!i.hasNext())
            return sb.append(']').toString();
         if (++counter >= COLLECTIONS_LIMIT) {
            return sb.append("...<")
                  .append(collection.size() - COLLECTIONS_LIMIT)
                  .append(" other elements>]").toString();
         }
         sb.append(", ");
      }
   }

   public static String printArray(byte[] array) {
      return printArray(array, false);
   }

   public static String printArray(byte[] array, boolean withHash) {
      if (array == null) return "null";

      int limit = 16;
      StringBuilder sb = new StringBuilder();
      sb.append("[B0x");
      if (array.length <= limit || IS_ARRAYS_DEBUG) {
         // Convert the entire byte array
         sb.append(toHexString(array));
         if (withHash) {
            sb.append(",h=");
            sb.append(Integer.toHexString(Arrays.hashCode(array)));
            sb.append(']');
         }
      } else {
         // Pick the first limit characters and convert that part
         sb.append(toHexString(array, limit));
         sb.append("..[");
         sb.append(array.length);
         if (withHash) {
            sb.append("],h=");
            sb.append(Integer.toHexString(Arrays.hashCode(array)));
         }
         sb.append(']');
      }
      return sb.toString();
   }

   public static String toHexString(byte[] input) {
      return toHexString(input, input != null ? input.length : 0);
   }

   public static String toHexString(byte[] input, int limit) {
      return toHexString(input, 0, limit);
   }

   public static String toHexString(byte[] input, int offset, int limit) {
      if (input == null)
         return "null";

      int length = Math.min(limit - offset, input.length - offset);
      char[] result = new char[length * 2];

      for (int i = 0; i < length; ++i) {
         result[2 * i] = HEX_VALUES.charAt((input[i + offset] >> 4) & 0x0F);
         result[2 * i + 1] = HEX_VALUES.charAt((input[i + offset] & 0x0F));
      }
      return String.valueOf(result);
   }

   public static String padString(String s, int minWidth) {
      if (s.length() < minWidth) {
         StringBuilder sb = new StringBuilder(s);
         while (sb.length() < minWidth) sb.append(" ");
         return sb.toString();
      }
      return s;
   }

   private static final String INDENT = "    ";

   public static String threadDump() {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      String timestamp = dateFormat.format(new Date());

      StringBuilder threadDump = new StringBuilder();
      threadDump.append(timestamp);
      threadDump.append("\nFull thread dump ");
      threadDump.append("\n");

      Thread.getAllStackTraces().forEach((thread, elements) -> {
         threadDump.append("\"").append(thread.getName())
               .append("\" nid=").append(thread.getId())
               .append(" state=").append(thread.getState())
               .append("\n");

         for (StackTraceElement e : elements) {
            threadDump.append(INDENT).append("at ").append(e.toString()).append("\n");
         }
      });
      return threadDump.toString();
   }

   public static CacheException rewrapAsCacheException(Throwable t) {
      if (t instanceof CompletionException)
         throw new IllegalArgumentException("CompletionException should never be wrapped");

      if (t instanceof CacheException)
         return (CacheException) t;
      else
         return new CacheException(t);
   }

   @SafeVarargs
   public static <T> Set<T> asSet(T... a) {
      if (a.length > 1)
         return new HashSet<>(Arrays.asList(a));
      else
         return Collections.singleton(a[0]);
   }

   /**
    * Prints the identity hash code of the object passed as parameter in an hexadecimal format in order to safe space.
    */
   public static String hexIdHashCode(Object o) {
      return Integer.toHexString(System.identityHashCode(o));
   }

   public static String hexDump(byte[] data) {
      return hexDump(data, 0, data.length);
   }

   public static String hexDump(ByteBuffer buffer) {
      return hexDump(buffer::get, buffer.position(), buffer.remaining());
   }

   public static String hexDump(byte[] buffer, int offset, int actualLength) {
      assert actualLength + offset <= buffer.length;
      int dumpLength = Math.min(actualLength, HEX_DUMP_LIMIT);
      StringBuilder sb = new StringBuilder(dumpLength * 2 + 30);
      for (int i = 0; i < dumpLength; ++i) {
         addHexByte(sb, buffer[offset + i]);
      }
      if (dumpLength < actualLength) {
         sb.append("...");
      }
      sb.append(" (").append(actualLength).append(" bytes)");
      return sb.toString();
   }

   public static String hexDump(ByteGetter byteGetter, int offset, int actualLength) {
      int dumpLength = Math.min(actualLength, HEX_DUMP_LIMIT);
      StringBuilder sb = new StringBuilder(dumpLength * 2 + 30);
      for (int i = 0; i < dumpLength; ++i) {
         addHexByte(sb, byteGetter.get(offset + i));
      }
      if (dumpLength < actualLength) {
         sb.append("...");
      }
      sb.append(" (").append(actualLength).append(" bytes)");
      return sb.toString();
   }

   public static void addHexByte(StringBuilder buf, byte b) {
      int offset = (b & 0xFF) * 2;
      buf.append(HEX_DUMP_CHARS, offset, 2);
   }

   private static void addSingleHexChar(StringBuilder buf, byte b) {
      buf.append(HEX_VALUES.charAt(b & 0x0F));
   }


   /**
    * Applies the given hash function to the hash code of a given object, and then normalizes it to ensure a positive
    * value is always returned.
    *
    * @param object  to hash
    * @param hashFct hash function to apply
    * @return a non-null, non-negative normalized hash code for a given object
    */
   public static int getNormalizedHash(Object object, Hash hashFct) {
      // make sure no negative numbers are involved.
      return hashFct.hash(object) & Integer.MAX_VALUE;
   }

   /**
    * Returns the size of each segment, given a number of segments. This assumes
    * a 32 bit hash is used and we ignore the most significant bit.
    *
    * @param numSegments number of segments required
    * @return the size of each segment
    */
   public static int getSegmentSize(int numSegments) {
      return (int) Math.ceil((double) (1L << 31) / numSegments);
   }

   public static int getSegmentSize(Hash hash, int numSegments) {
      return getSegmentSize(hash.maxHashBits(), numSegments);
   }

   public static int getSegmentSize(int maxHashBits, int numSegments) {
      assert maxHashBits <= 32;
      // If using all 32 bits we ignore one as we don't want negative segments
      if (maxHashBits == 32) {
         maxHashBits--;
      }
      return (int) Math.ceil((double) (1L << maxHashBits) / numSegments);
   }

   /**
    * Returns whether the provided integer is a power of two or not. That is any number that is divisible by
    * two even if negative.
    * @param n the number to test
    * @return whether the number is a power of two or not.
    */
   public static boolean isPow2(int n) {
      // Just in case it is negative
      n = Math.abs(n);
      // If `n` is pow2 the binary is a 1 followed by only zeroes and `n - 1` is a 0 followed by only ones.
      // The binary and always resolves to 0.
      return (n & (n - 1)) == 0;
   }

   public static String join(List<String> strings, String separator) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;

      for (String string : strings) {
         if (!first) {
            sb.append(separator);
         } else {
            first = false;
         }
         sb.append(string);
      }

      return sb.toString();
   }

   /**
    * Returns a number such that the number is a power of two that is equal to, or greater than, the number passed in as
    * an argument.  The smallest number returned will be 1. Due to having to be a power of two, the highest int this can
    * return is 2<sup>31</sup> since int is signed.
    */
   public static int findNextHighestPowerOfTwo(int num) {
      if (num <= 1) {
         return 1;
      } else if (num >= 0x40000000) {
         return 0x40000000;
      }
      int highestBit = Integer.highestOneBit(num);
      return num <= highestBit ? highestBit : highestBit << 1;
   }

   /**
    * A function that calculates hash code of a byte array based on its contents but using the given size parameter as
    * deliminator for the content.
    */
   public static int hashCode(byte[] bytes, int size) {
      int contentLimit = size;
      if (size > bytes.length)
         contentLimit = bytes.length;

      int hashCode = 1;
      for (int i = 0; i < contentLimit; i++)
         hashCode = 31 * hashCode + bytes[i];

      return hashCode;
   }

   /**
    * Prints {@link Subject}'s principals as a one-liner (as opposed to default Subject's <code>toString()</code>
    * method, which prints every principal on separate line).
    */
   public static String prettyPrintSubject(Subject subject) {
      return (subject == null) ? "null" : "Subject with principal(s): " + toStr(subject.getPrincipals());
   }

   /**
    * Uses a {@link ThreadLocalRandom} to generate a UUID. Faster, but not secure
    */
   public static UUID threadLocalRandomUUID() {
      byte[] data = new byte[16];
      ThreadLocalRandom.current().nextBytes(data);
      data[6] &= 0x0f; /* clear version */
      data[6] |= 0x40; /* set to version 4 */
      data[8] &= 0x3f; /* clear variant */
      data[8] |= 0x80; /* set to IETF variant */
      long msb = 0;
      long lsb = 0;
      for (int i = 0; i < 8; i++)
         msb = (msb << 8) | (data[i] & 0xff);
      for (int i = 8; i < 16; i++)
         lsb = (lsb << 8) | (data[i] & 0xff);
      return new UUID(msb, lsb);
   }

   public static String unicodeEscapeString(String s) {
      int len = s.length();
      StringBuilder out = new StringBuilder(len * 2);

      for (int x = 0; x < len; x++) {
         char aChar = s.charAt(x);
         if ((aChar > 61) && (aChar < 127)) {
            if (aChar == '\\') {
               out.append('\\');
               out.append('\\');
               continue;
            }
            out.append(aChar);
            continue;
         }
         switch (aChar) {
            case ' ':
               if (x == 0)
                  out.append('\\');
               out.append(' ');
               break;
            case '\t':
               out.append('\\');
               out.append('t');
               break;
            case '\n':
               out.append('\\');
               out.append('n');
               break;
            case '\r':
               out.append('\\');
               out.append('r');
               break;
            case '\f':
               out.append('\\');
               out.append('f');
               break;
            case '=':
            case ':':
            case '#':
            case '!':
               out.append('\\');
               out.append(aChar);
               break;
            default:
               if ((aChar < 0x0020) || (aChar > 0x007e)) {
                  out.append('\\');
                  out.append('u');
                  addSingleHexChar(out, (byte) ((aChar >> 12) & 0xF));
                  addSingleHexChar(out, (byte) ((aChar >> 8) & 0xF));
                  addSingleHexChar(out, (byte) ((aChar >> 4) & 0xF));
                  addSingleHexChar(out, (byte) (aChar & 0xF));
               } else {
                  out.append(aChar);
               }
         }
      }
      return out.toString();
   }

   public static String unicodeUnescapeString(String s) {
      int len = s.length();
      StringBuilder out = new StringBuilder(len);

      for (int x = 0; x < len; x++) {
         char ch = s.charAt(x);
         if (ch == '\\') {
            ch = s.charAt(++x);
            if (ch == 'u') {
               int value = 0;
               for (int i = 0; i < 4; i++) {
                  ch = s.charAt(++x);
                  if (ch >= '0' && ch <= '9') {
                     value = (value << 4) + ch - '0';
                  } else if (ch >= 'a' && ch <= 'f') {
                     value = (value << 4) + 10 + ch - 'a';
                  } else if (ch >= 'A' && ch <= 'F') {
                     value = (value << 4) + 10 + ch - 'A';
                  } else throw new IllegalArgumentException("Malformed \\uxxxx encoding.");
               }
               out.append((char) value);
            } else {
               if (ch == 't')
                  ch = '\t';
               else if (ch == 'r')
                  ch = '\r';
               else if (ch == 'n')
                  ch = '\n';
               else if (ch == 'f')
                  ch = '\f';
               out.append(ch);
            }
         } else {
            out.append(ch);
         }
      }
      return out.toString();
   }

   public static <T> Supplier<T> getInstanceSupplier(String className, ClassLoader classLoader) {
      return () -> getInstance(className, classLoader);
   }

   /**
    * Deletes directory recursively.
    *
    * @param directoryName Directory to be deleted
    */
   public static void recursiveFileRemove(String directoryName) {
      File file = new File(directoryName);
      recursiveFileRemove(file);
   }

   public static void recursiveFileRemove(Path path) {
      recursiveFileRemove(path.toFile());
   }

   /**
    * Deletes directory recursively.
    *
    * @param directory Directory to be deleted
    */
   public static void recursiveFileRemove(File directory) {
      if (directory.exists()) {
         log.tracef("Deleting file %s", directory);
         recursiveDelete(directory);
      }
   }

   private static void recursiveDelete(File f) {
      try {
         Files.walkFileTree(f.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
               Files.delete(file);
               return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
               if (e == null) {
                  Files.delete(dir);
                  return FileVisitResult.CONTINUE;
               } else {
                  throw e;
               }
            }
         });
      } catch (Exception e) {
         throw new IllegalStateException(e);
      }
   }

   public static void recursiveDirectoryCopy(Path source, Path target) throws IOException {
      Files.walkFileTree(source, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new FileVisitor<Path>() {
         @Override
         public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            try {
               if (!source.equals(dir)) {
                  String relativize = source.relativize(dir).toString();
                  Path resolve = target.resolve(relativize);
                  Files.copy(dir, resolve);
               }
            } catch (FileAlreadyExistsException x) {
               // do nothing
            } catch (IOException x) {
               return FileVisitResult.SKIP_SUBTREE;
            }
            return FileVisitResult.CONTINUE;
         }

         @Override
         public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.copy(file, target.resolve(source.relativize(file).toString()));
            return FileVisitResult.CONTINUE;
         }

         @Override
         public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.CONTINUE;
         }

         @Override
         public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
            return FileVisitResult.CONTINUE;
         }
      });
   }

   public static char[] toCharArray(String s) {
      return s == null ? null : s.toCharArray();
   }

   public static Object[] objectArray(int length) {
      return length == 0 ? EMPTY_OBJECT_ARRAY : new Object[length];
   }

   public static String[] stringArray(int length) {
      return length == 0 ? EMPTY_STRING_ARRAY : new String[length];
   }

   public static Throwable[] throwableArray(int length) {
      return length == 0 ? EMPTY_THROWABLE_ARRAY : new Throwable[length];
   }

   public static void renameTempFile(File tempFile, File lockFile, File dstFile)
         throws IOException {
      FileLock lock = null;
      try (FileOutputStream lockFileOS = new FileOutputStream(lockFile)) {
         lock = lockFileOS.getChannel().lock();
         Files.move(tempFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      } finally {
         if (lock != null && lock.isValid()) {
            lock.release();
         }
         if (!lockFile.delete()) {
            log.debugf("Unable to delete lock file %s", lockFile);
         }
      }
   }

   public static Throwable getRootCause(Throwable re) {
      if (re == null) return null;
      Throwable cause = re.getCause();
      if (cause != null)
         return getRootCause(cause);
      else
         return re;
   }

   public static Marshaller getJBossMarshaller(ClassLoader classLoader, ClassAllowList classAllowList) {
      try {
         Class<?> marshallerClass = classLoader.loadClass(GENERIC_JBOSS_MARSHALLING_CLASS);
         return Util.newInstanceOrNull(marshallerClass.asSubclass(Marshaller.class),
               new Class[]{ClassLoader.class, ClassAllowList.class}, classLoader, classAllowList);
      } catch (ClassNotFoundException e) {
         return null;
      }
   }

   // TODO: Replace with Objects.requireNonNullElse(T obj, T defaultObj) when upgrading to JDK 9+
   public static <T> T requireNonNullElse(T obj, T defaultObj) {
      return (obj != null) ? obj : Objects.requireNonNull(defaultObj, "defaultObj");
   }

   public static void longToBytes(long val, byte[] array, int offset) {
      for (int i = 7; i > 0; i--) {
         array[offset + i] = (byte) val;
         val >>>= 8;
      }
      array[offset] = (byte) val;
   }

   public static String unquote(String s) {
      if (s.charAt(0) == '"' || s.charAt(0) == '\'') {
         return s.substring(1, s.length() - 1);
      } else {
         return s;
      }
   }

   public static Object fromString(Class<?> klass, String value) {
      if (value == null) {
         return null;
      }
      if (klass == Character.class) {
         if (value.length() == 1) {
            return value.charAt(0);
         } else {
            throw new IllegalArgumentException("Expected a single character, got '" + value + "'");
         }
      } else if (klass == Byte.class) {
         return Byte.valueOf(value);
      } else if (klass == Short.class) {
         return Short.valueOf(value);
      } else if (klass == Integer.class) {
         return Integer.valueOf(value);
      } else if (klass == Long.class) {
         return Long.valueOf(value);
      } else if (klass == Boolean.class) {
         return parseBoolean(value);
      } else if (klass == String.class) {
         return value;
      } else if (klass == String[].class) {
         return value.isEmpty() ? new String[]{} : value.split(" ");
      } else if (klass == Set.class) {
         return value.isEmpty() ? new HashSet<>() : new HashSet<>(Arrays.asList(value.split(" ")));
      } else if (klass == List.class) {
         return value.isEmpty() ? new ArrayList<>() : Arrays.asList(value.split(" "));
      } else if (klass == char[].class) {
         return value.toCharArray();
      } else if (klass == Float.class) {
         return Float.valueOf(value);
      } else if (klass == Double.class) {
         return Double.valueOf(value);
      } else if (klass == BigDecimal.class) {
         return new BigDecimal(value);
      } else if (klass == BigInteger.class) {
         return new BigInteger(value);
      } else if (klass == File.class) {
         return new File(value);
      } else if (klass.isEnum()) {
         return parseEnum((Class)klass, value);
      } else if (klass == Properties.class) {
         try {
            Properties props = new Properties();
            props.load(new ByteArrayInputStream(value.getBytes()));
            return props;
         } catch (IOException e) {
            throw new CacheConfigurationException("Failed to load Properties from: " + value, e);
         }
      }

      throw new CacheConfigurationException("Cannot convert " + value + " to type " + klass.getName());
   }

   public static boolean parseBoolean(String value) {
      return switch (value.toLowerCase()) {
         case "true", "yes", "y", "on" -> true;
         case "false", "no", "n", "off" -> false;
         default -> throw Log.CONFIG.illegalBooleanValue(value);
      };
   }

   public static <T extends Enum<T>> T parseEnum(Class<T> enumClass, String value) {
      try {
         return Enum.valueOf(enumClass, value);
      } catch (IllegalArgumentException e) {
         throw Log.CONFIG.illegalEnumValue(value, EnumSet.allOf(enumClass));
      }
   }

   public static void unwrapSuppressed(Throwable t, Throwable t1) {
      if (t1.getSuppressed().length > 0) {
         for (Throwable suppressed : t1.getSuppressed()) {
            t.addSuppressed(suppressed);
         }
      } else {
         t.addSuppressed(t1);
      }
   }

   public static String unwrapExceptionMessage(Throwable t) {
      // Avoid duplicate messages
      LinkedHashSet<String> messages = new LinkedHashSet<>();
      String rootMessage = t.getMessage();
      if (rootMessage != null) {
         messages.add(rootMessage);
      }
      for(Throwable suppressed : t.getSuppressed()) {
         messages.add(suppressed.getMessage());
      }
      return String.join("\n    ", messages);
   }

   /**
    * Returns the byte at {@code index}.
    */
   public interface ByteGetter {
      byte get(int index);
   }

   /**
    * This method is to be replaced by Java 9 Arrays#equals with the same arguments.
    *
    * @param a          first array to test contents
    * @param aFromIndex the offset into the first array to start comparison
    * @param aToIndex   the last element (exclusive) of the first array to compare
    * @param b          second array to test contents
    * @param bFromIndex the offset into the second array to start comparison
    * @param bToIndex   the last element (exclusive) of the second array to compare
    * @return if the bytes in the two array ranges are equal
    */
   public static boolean arraysEqual(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
      int totalAmount = aToIndex - aFromIndex;
      if (totalAmount != bToIndex - bFromIndex) {
         return false;
      }
      for (int i = 0; i < totalAmount; ++i) {
         if (a[aFromIndex + i] != b[bFromIndex + i]) {
            return false;
         }
      }
      return true;
   }

   public static byte[] concat(byte[] a, byte[] b) {
      int aLen = a.length;
      int bLen = b.length;
      byte[] ret = new byte[aLen + bLen];
      System.arraycopy(a, 0, ret, 0, aLen);
      System.arraycopy(b, 0, ret, aLen, bLen);
      return ret;
   }

   public static <T> T[] concat(T[] a, T b) {
      int aLen = a.length;
      T[] ret = Arrays.copyOf(a, aLen + 1);
      ret[aLen] = b;
      return ret;
   }

   public static RuntimeException unchecked(Throwable t) {
      if (t instanceof RuntimeException) {
         return (RuntimeException) t;
      } else {
         return new RuntimeException(t);
      }
   }
}
