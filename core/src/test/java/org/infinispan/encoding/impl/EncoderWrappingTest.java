package org.infinispan.encoding.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.encoding.DataConversion;
import org.infinispan.metadata.Metadata;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * @since 15.0
 **/

@Test(groups = "unit", testName = "encoding.impl.StorageConfigurationManagerTest", enabled = false)
public class EncoderWrappingTest {
   static final byte[] TEST_KEY = new byte[]{65, 66, 67, 68};
   static final byte[] TEST_VALUE = new byte[]{69, 70, 71, 72};
   static final Metadata TEST_METADATA = new Metadata() {
      @Override
      public long lifespan() {
         return 0;
      }

      @Override
      public long maxIdle() {
         return 0;
      }

      @Override
      public EntryVersion version() {
         return null;
      }

      @Override
      public Builder builder() {
         return null;
      }
   };

   public void testEncoder() {
      AdvancedCache mock = Mockito.mock(AdvancedCache.class);
      DataConversion keyConversion = DataConversion.newKeyDataConversion().withRequestMediaType(MediaType.APPLICATION_OCTET_STREAM);
      DataConversion valueConversion = DataConversion.newKeyDataConversion().withRequestMediaType(MediaType.APPLICATION_OCTET_STREAM);
      EncoderCache<byte[], byte[]> cache = new EncoderCache<>(mock, null, null, keyConversion, valueConversion);
      Method[] methods = AdvancedCache.class.getMethods();
      for (Method method : methods) {
         Parameter[] parameters = method.getParameters();
         if (parameters.length > 0) {
            Object[] args = new Object[parameters.length];
            for (int i = 0; i < parameters.length; i++) {
               Parameter parameter = parameters[i];
               switch (parameter.getName()) {
                  case "key":
                     args[i] = TEST_KEY;
                     break;
                  case "keys":
                     if (parameter.isVarArgs()) {
                        args[i] = new Object[]{TEST_KEY};
                     } else {
                        System.out.println("XXXX");
                     }
                     break;
                  case "value":
                  case "newValue":
                  case "oldValue":
                     args[i] = TEST_VALUE;
                     break;
                  case "metadata":
                     args[i] = TEST_METADATA;
                     break;
                  case "unit":
                  case "lifespanUnit":
                  case "maxIdleUnit":
                  case "maxIdleTimeUnit":
                     args[i] = TimeUnit.DAYS;
                     break;
                  case "lifespan":
                  case "maxIdle":
                  case "maxIdleTime":
                     args[i] = Long.valueOf(1);
                     break;
                  case "mappingFunction":
                  case "remappingFunction":
                     args[i] = null;
                     break;
                  default:
                     System.out.printf("%s(%s %s);%n", method.getName(), parameter.getType(), parameter.getName());
                     args[i] = null;
                     break;
               }
            }
            try {
               method.invoke(cache, args);
            } catch (IllegalAccessException e) {
               throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
               throw new RuntimeException(e);
            }
         }
      }
   }
}
