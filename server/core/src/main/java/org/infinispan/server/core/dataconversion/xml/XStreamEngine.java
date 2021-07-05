package org.infinispan.server.core.dataconversion.xml;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.basic.BigDecimalConverter;
import com.thoughtworks.xstream.converters.basic.BigIntegerConverter;
import com.thoughtworks.xstream.converters.basic.BooleanConverter;
import com.thoughtworks.xstream.converters.basic.ByteConverter;
import com.thoughtworks.xstream.converters.basic.CharConverter;
import com.thoughtworks.xstream.converters.basic.DateConverter;
import com.thoughtworks.xstream.converters.basic.DoubleConverter;
import com.thoughtworks.xstream.converters.basic.FloatConverter;
import com.thoughtworks.xstream.converters.basic.IntConverter;
import com.thoughtworks.xstream.converters.basic.LongConverter;
import com.thoughtworks.xstream.converters.basic.NullConverter;
import com.thoughtworks.xstream.converters.basic.ShortConverter;
import com.thoughtworks.xstream.converters.basic.StringConverter;
import com.thoughtworks.xstream.converters.basic.URIConverter;
import com.thoughtworks.xstream.converters.basic.URLConverter;
import com.thoughtworks.xstream.converters.collections.ArrayConverter;
import com.thoughtworks.xstream.converters.collections.BitSetConverter;
import com.thoughtworks.xstream.converters.collections.CharArrayConverter;
import com.thoughtworks.xstream.converters.collections.CollectionConverter;
import com.thoughtworks.xstream.converters.collections.MapConverter;
import com.thoughtworks.xstream.converters.collections.SingletonCollectionConverter;
import com.thoughtworks.xstream.converters.collections.SingletonMapConverter;
import com.thoughtworks.xstream.converters.extended.EncodedByteArrayConverter;
import com.thoughtworks.xstream.converters.extended.FileConverter;
import com.thoughtworks.xstream.converters.extended.GregorianCalendarConverter;
import com.thoughtworks.xstream.converters.extended.JavaClassConverter;
import com.thoughtworks.xstream.converters.extended.JavaFieldConverter;
import com.thoughtworks.xstream.converters.extended.JavaMethodConverter;
import com.thoughtworks.xstream.converters.extended.LocaleConverter;
import com.thoughtworks.xstream.converters.reflection.ExternalizableConverter;
import com.thoughtworks.xstream.converters.reflection.ReflectionConverter;
import com.thoughtworks.xstream.converters.reflection.SerializableConverter;

/**
 * Adapter for the XStream XML Engine with pre-defined configurations.
 *
 * @since 13.0
 */
public class XStreamEngine extends XStream {

   public XStreamEngine() {
      super(new MXParserDriver());
   }

   @Override
   protected void setupConverters() {
      registerConverter(new ReflectionConverter(getMapper(), getReflectionProvider()), PRIORITY_VERY_LOW);
      registerConverter(new SerializableConverter(getMapper(), getReflectionProvider(), getClassLoaderReference()), PRIORITY_LOW);
      registerConverter(new ExternalizableConverter(getMapper(), getClassLoaderReference()), PRIORITY_LOW);
      registerConverter(new NullConverter(), PRIORITY_VERY_HIGH);
      registerConverter(new IntConverter(), PRIORITY_NORMAL);
      registerConverter(new FloatConverter(), PRIORITY_NORMAL);
      registerConverter(new DoubleConverter(), PRIORITY_NORMAL);
      registerConverter(new LongConverter(), PRIORITY_NORMAL);
      registerConverter(new ShortConverter(), PRIORITY_NORMAL);
      registerConverter((Converter) new CharConverter(), PRIORITY_NORMAL);
      registerConverter(new BooleanConverter(), PRIORITY_NORMAL);
      registerConverter(new ByteConverter(), PRIORITY_NORMAL);
      registerConverter(new StringConverter(), PRIORITY_NORMAL);
      registerConverter(new DateConverter(), PRIORITY_NORMAL);
      registerConverter(new BitSetConverter(), PRIORITY_NORMAL);
      registerConverter(new URIConverter(), PRIORITY_NORMAL);
      registerConverter(new URLConverter(), PRIORITY_NORMAL);
      registerConverter(new BigIntegerConverter(), PRIORITY_NORMAL);
      registerConverter(new BigDecimalConverter(), PRIORITY_NORMAL);
      registerConverter(new ArrayConverter(getMapper()), PRIORITY_NORMAL);
      registerConverter(new CharArrayConverter(), PRIORITY_NORMAL);
      registerConverter(new CollectionConverter(getMapper()), PRIORITY_NORMAL);
      registerConverter(new MapConverter(getMapper()), PRIORITY_NORMAL);
      registerConverter(new SingletonCollectionConverter(getMapper()), PRIORITY_NORMAL);
      registerConverter(new SingletonMapConverter(getMapper()), PRIORITY_NORMAL);
      registerConverter((Converter) new EncodedByteArrayConverter(), PRIORITY_NORMAL);
      registerConverter(new FileConverter(), PRIORITY_NORMAL);
      registerConverter(new JavaClassConverter(getClassLoaderReference()), PRIORITY_NORMAL);
      registerConverter(new JavaMethodConverter(getClassLoaderReference()), PRIORITY_NORMAL);
      registerConverter(new JavaFieldConverter(getClassLoaderReference()), PRIORITY_NORMAL);
      registerConverter(new LocaleConverter(), PRIORITY_NORMAL);
      registerConverter(new GregorianCalendarConverter(), PRIORITY_NORMAL);
   }
}
