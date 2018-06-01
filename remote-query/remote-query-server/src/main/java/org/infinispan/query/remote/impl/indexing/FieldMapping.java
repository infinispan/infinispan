package org.infinispan.query.remote.impl.indexing;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.LuceneOptions;
import org.hibernate.search.bridge.builtin.BooleanBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.StringBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.bridge.spi.NullMarker;
import org.hibernate.search.bridge.util.impl.ToStringNullMarker;
import org.hibernate.search.bridge.util.impl.TwoWayString2FieldBridgeAdaptor;
import org.hibernate.search.engine.nulls.codec.impl.LuceneIntegerNullMarkerCodec;
import org.hibernate.search.engine.nulls.codec.impl.LuceneLongNullMarkerCodec;
import org.hibernate.search.engine.nulls.codec.impl.LuceneStringNullMarkerCodec;
import org.hibernate.search.engine.nulls.codec.impl.NullMarkerCodec;
import org.infinispan.protostream.descriptors.EnumValueDescriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;

/**
 * A mapping from an object field to an index field and the flags that enable indexing, storage and analysis.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FieldMapping {

   private static final NullMarkerCodec NOT_ENCODING_NULL = new NullMarkerCodec() {

      @Override
      public NullMarker getNullMarker() {
         return null;
      }

      @Override
      public void encodeNullValue(String fieldName, Document document, LuceneOptions luceneOptions) {
      }

      @Override
      public Query createNullMatchingQuery(String fieldName) {
         throw new IllegalStateException("Cannot build IS NULL query for field '" + fieldName + "' which does not have indexNullAs configured for indexing null values.");
      }

      @Override
      public boolean representsNullValue(IndexableField field) {
         return field == null;
      }
   };

   private static final LuceneStringNullMarkerCodec STRING_NULL_MARKER_CODEC = new LuceneStringNullMarkerCodec(new ToStringNullMarker(IndexingMetadata.DEFAULT_NULL_TOKEN));

   private static final FieldBridge DOUBLE_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.DOUBLE_FIELD_BRIDGE, STRING_NULL_MARKER_CODEC);

   private static final FieldBridge FLOAT_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.FLOAT_FIELD_BRIDGE, STRING_NULL_MARKER_CODEC);

   private static final FieldBridge LONG_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.LONG_FIELD_BRIDGE, STRING_NULL_MARKER_CODEC);

   private static final FieldBridge INT_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.INT_FIELD_BRIDGE, STRING_NULL_MARKER_CODEC);

   private static final FieldBridge STRING_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE), STRING_NULL_MARKER_CODEC);

   private static final FieldBridge BOOL_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(new BooleanBridge()), STRING_NULL_MARKER_CODEC);

   /**
    * The name of the field in the index.
    */
   private final String name;

   /**
    * Enable indexing.
    */
   private final boolean index;

   private final float boost;

   /**
    * Enable analysis.
    */
   private final boolean analyze;

   /**
    * Enable storage.
    */
   private final boolean store;

   private final boolean sortable;

   /**
    * The name of the analyzer definition.
    */
   private final String analyzer;

   private final String indexNullAs;

   private final LuceneOptions luceneOptions;

   private final FieldDescriptor fieldDescriptor;

   /**
    * A legacy field definition using IndexedField annotation.
    */
   private final boolean isLegacy;

   /**
    * Indicates if lazy initialization of {@link #indexNullAsObj} and {@link #fieldBridge} fields was performed or not.
    */
   private volatile boolean isInitialized = false;

   private Object indexNullAsObj;

   private FieldBridge fieldBridge;

   FieldMapping(String name, boolean index, float boost, boolean analyze, boolean store, boolean sortable,
                String analyzer, String indexNullAs,
                LuceneOptions luceneOptions,
                FieldDescriptor fieldDescriptor,
                boolean isLegacy) {
      if (name == null) {
         throw new IllegalArgumentException("name argument cannot be null");
      }
      if (luceneOptions == null) {
         throw new IllegalArgumentException("luceneOptions argument cannot be null");
      }
      if (fieldDescriptor == null) {
         throw new IllegalArgumentException("fieldDescriptor argument cannot be null");
      }
      this.name = name;
      this.index = index;
      this.boost = boost;
      this.analyze = analyze;
      this.store = store;
      this.sortable = sortable;
      this.analyzer = analyzer;
      this.indexNullAs = indexNullAs;
      this.fieldDescriptor = fieldDescriptor;
      this.luceneOptions = luceneOptions;
      this.isLegacy = isLegacy;
   }

   public String name() {
      return name;
   }

   public boolean index() {
      return index;
   }

   public float boost() {
      return boost;
   }

   public boolean analyze() {
      return analyze;
   }

   public boolean store() {
      return store;
   }

   public boolean sortable() {
      return sortable;
   }

   public String analyzer() {
      return analyzer;
   }

   public LuceneOptions luceneOptions() {
      return luceneOptions;
   }

   public Object indexNullAs() {
      init();
      return indexNullAsObj;
   }

   public FieldBridge fieldBridge() {
      init();
      return fieldBridge;
   }

   public boolean isLegacy() {
      return isLegacy;
   }

   private void init() {
      if (!isInitialized) {
         if (fieldDescriptor.getType() == null) {
            // this could only happen due to a programming error
            throw new IllegalStateException("FieldDescriptors are not fully initialised!");
         }
         indexNullAsObj = parseIndexNullAs();
         fieldBridge = makeFieldBridge();
         isInitialized = true;
      }
   }

   private Object parseIndexNullAs() {
      // a legacy @IndexedField is handled differently to maintain backward compatibility: the value is never parsed so only string null tokens are supported.
      if (indexNullAs != null && !isLegacy) {
         switch (fieldDescriptor.getType()) {
            case DOUBLE:
               return Double.parseDouble(indexNullAs);
            case FLOAT:
               return Float.parseFloat(indexNullAs);
            case INT64:
            case UINT64:
            case FIXED64:
            case SFIXED64:
            case SINT64:
               return Long.parseLong(indexNullAs);
            case INT32:
            case FIXED32:
            case UINT32:
            case SFIXED32:
            case SINT32:
               return Integer.parseInt(indexNullAs);
            case ENUM:
               EnumValueDescriptor enumVal = fieldDescriptor.getEnumType().findValueByName(indexNullAs);
               if (enumVal == null) {
                  throw new IllegalArgumentException("Enum value not found : " + indexNullAs);
               }
               return enumVal.getNumber();
            case BOOL:
               return Boolean.valueOf(indexNullAs);
         }
      }
      return indexNullAs;
   }

   private FieldBridge makeFieldBridge() {
      if (isLegacy) {
         return getDefaultFieldBridge(fieldDescriptor.getType());
      }

      switch (fieldDescriptor.getType()) {
         case DOUBLE:
            return indexNullAsObj == null ?
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.DOUBLE_FIELD_BRIDGE, NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.DOUBLE_FIELD_BRIDGE, new LuceneLongNullMarkerCodec(new ToStringNullMarker(indexNullAsObj)));
         case FLOAT:
            return indexNullAsObj == null ?
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.FLOAT_FIELD_BRIDGE, NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.FLOAT_FIELD_BRIDGE, new LuceneLongNullMarkerCodec(new ToStringNullMarker(indexNullAsObj)));
         case INT64:
         case UINT64:
         case FIXED64:
         case SFIXED64:
         case SINT64:
            return indexNullAsObj == null ?
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.LONG_FIELD_BRIDGE, NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.LONG_FIELD_BRIDGE, new LuceneLongNullMarkerCodec(new ToStringNullMarker(indexNullAsObj)));
         case INT32:
         case FIXED32:
         case UINT32:
         case SFIXED32:
         case SINT32:
         case ENUM:
            return indexNullAsObj == null ?
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.INT_FIELD_BRIDGE, NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.INT_FIELD_BRIDGE, new LuceneIntegerNullMarkerCodec(new ToStringNullMarker(indexNullAsObj)));
         case BOOL:
            return indexNullAsObj == null ?
                  new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(new BooleanBridge()), NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(new BooleanBridge()), new LuceneStringNullMarkerCodec(new ToStringNullMarker(indexNullAsObj)));
         default:
            return indexNullAsObj == null ?
                  new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE), NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE), new LuceneStringNullMarkerCodec(new ToStringNullMarker(indexNullAsObj)));
      }
   }

   public static FieldBridge getDefaultFieldBridge(Type type) {
      switch (type) {
         case DOUBLE:
            return DOUBLE_FIELD_BRIDGE;
         case FLOAT:
            return FLOAT_FIELD_BRIDGE;
         case INT64:
         case UINT64:
         case FIXED64:
         case SFIXED64:
         case SINT64:
            return LONG_FIELD_BRIDGE;
         case INT32:
         case FIXED32:
         case UINT32:
         case SFIXED32:
         case SINT32:
         case ENUM:
            return INT_FIELD_BRIDGE;
         case BOOL:
            return BOOL_FIELD_BRIDGE;
         case STRING:
         case BYTES:
         case GROUP:
         case MESSAGE:
            return STRING_FIELD_BRIDGE;
      }
      return null;
   }

   @Override
   public String toString() {
      return "FieldMapping{" +
            "name='" + name + '\'' +
            ", index=" + index +
            ", boost=" + boost +
            ", analyze=" + analyze +
            ", store=" + store +
            ", sortable=" + sortable +
            ", analyzer='" + analyzer + '\'' +
            ", indexNullAs=" + indexNullAs +
            ", luceneOptions=" + luceneOptions +
            ", isLegacy=" + isLegacy +
            '}';
   }
}
