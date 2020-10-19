package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;
import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.util.Immutables;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;


/**
 * Represent a content type with optional parameters.
 *
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.MEDIA_TYPE)
@SerializeWith(value = MediaType.MediaTypeExternalizer.class)
public final class MediaType {
   private static final Pattern TREE_PATTERN;
   private static final Pattern LIST_SEPARATOR_PATTERN;

   static {
      // Adapted from https://stackoverflow.com/a/48046041/55870
      // See also https://tools.ietf.org/html/rfc7231#section-3.1.1.1
      // and https://tools.ietf.org/html/rfc7230#section-3.2.6
      // Extended to support "*" as a media type (as used by java.net.HttpURLConnection)
      // More details at https://bugs.openjdk.java.net/browse/JDK-8163921
      // Use PrintPattern.main(new String(TREE_PATTERN.toString()) to view the regex structure
      String ows = "[ \t]*";
      // Expand ranges in token pattern so that it uses the ASCII-only BitClass for matching
      String token = "[!#$%&'*+.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz^_`|~-]+";
      String quotedString = "\"(?:[^\"\\\\]|\\\\.)*\"";
      String typeSubtype = ows + "((" + token + ")/" + token + "|\\*)" + ows;
      String parameter = ";" + ows + "(" + token + ")=(" + token + "|" + quotedString + ")" + ows;
      String listSeparator = "\\G," + ows;
      String tree = "^" + typeSubtype + "|\\G" + parameter + "|\\G" + listSeparator;
      TREE_PATTERN = Pattern.compile(tree, Pattern.DOTALL);
      LIST_SEPARATOR_PATTERN = Pattern.compile(listSeparator, Pattern.DOTALL);
   }

   // OpenMetrics aka Prometheus content type
   public static final String APPLICATION_OPENMETRICS_TYPE = "application/openmetrics-text";
   public static final String APPLICATION_JAVASCRIPT_TYPE = "application/javascript";
   public static final String APPLICATION_JSON_TYPE = "application/json";
   public static final String APPLICATION_OCTET_STREAM_TYPE = "application/octet-stream";
   public static final String APPLICATION_OBJECT_TYPE = "application/x-java-object";
   public static final String APPLICATION_PDF_TYPE = "application/pdf";
   public static final String APPLICATION_RTF_TYPE = "application/rtf";
   public static final String APPLICATION_SERIALIZED_OBJECT_TYPE = "application/x-java-serialized-object";
   public static final String APPLICATION_XML_TYPE = "application/xml";
   public static final String APPLICATION_ZIP_TYPE = "application/zip";
   public static final String APPLICATION_JBOSS_MARSHALLING_TYPE = "application/x-jboss-marshalling";
   public static final String APPLICATION_PROTOSTREAM_TYPE = "application/x-protostream";
   /**
    * @deprecated Since 11.0, without replacement.
    */
   @Deprecated
   public static final String APPLICATION_UNKNOWN_TYPE = "application/unknown";
   public static final String WWW_FORM_URLENCODED_TYPE = "application/x-www-form-urlencoded";
   public static final String IMAGE_GIF_TYPE = "image/gif";
   public static final String IMAGE_JPEG_TYPE = "image/jpeg";
   public static final String IMAGE_PNG_TYPE = "image/png";
   public static final String MULTIPART_FORM_DATA_TYPE = "multipart/form-data";
   public static final String TEXT_CSS_TYPE = "text/css";
   public static final String TEXT_CSV_TYPE = "text/csv";
   public static final String TEXT_PLAIN_TYPE = "text/plain";
   public static final String TEXT_HTML_TYPE = "text/html";
   /**
    * @deprecated Since 11.0, will be removed with ISPN-9622
    */
   @Deprecated
   public static final String APPLICATION_INFINISPAN_MARSHALLING_TYPE = "application/x-infinispan-marshalling";
   /**
    * @deprecated Since 11.0, will be removed in 14.0. No longer used for BINARY storage.
    */
   @Deprecated
   public static final String APPLICATION_INFINISPAN_BINARY_TYPE = "application/x-infinispan-binary";
   public static final String APPLICATION_PROTOSTUFF_TYPE = "application/x-protostuff";
   public static final String APPLICATION_KRYO_TYPE = "application/x-kryo";
   public static final String MATCH_ALL_TYPE = "*/*";

   // OpenMetrics aka Prometheus content type
   public static final MediaType APPLICATION_OPENMETRICS = fromString(APPLICATION_OPENMETRICS_TYPE);
   public static final MediaType APPLICATION_JAVASCRIPT = fromString(APPLICATION_JAVASCRIPT_TYPE);
   public static final MediaType APPLICATION_JSON = fromString(APPLICATION_JSON_TYPE);
   public static final MediaType APPLICATION_OCTET_STREAM = fromString(APPLICATION_OCTET_STREAM_TYPE);
   public static final MediaType APPLICATION_OBJECT = fromString(APPLICATION_OBJECT_TYPE);
   public static final MediaType APPLICATION_SERIALIZED_OBJECT = fromString(APPLICATION_SERIALIZED_OBJECT_TYPE);
   public static final MediaType APPLICATION_XML = fromString(APPLICATION_XML_TYPE);
   public static final MediaType APPLICATION_PROTOSTREAM = fromString(APPLICATION_PROTOSTREAM_TYPE);
   public static final MediaType APPLICATION_JBOSS_MARSHALLING = fromString(APPLICATION_JBOSS_MARSHALLING_TYPE);
   /**
    * @deprecated Since 11.0, will be removed with ISPN-9622
    */
   @Deprecated
   public static final MediaType APPLICATION_INFINISPAN_MARSHALLED = fromString(APPLICATION_INFINISPAN_MARSHALLING_TYPE);
   public static final MediaType APPLICATION_WWW_FORM_URLENCODED = fromString(WWW_FORM_URLENCODED_TYPE);
   public static final MediaType IMAGE_PNG = fromString(IMAGE_PNG_TYPE);
   public static final MediaType MULTIPART_FORM_DATA = fromString(MULTIPART_FORM_DATA_TYPE);
   public static final MediaType TEXT_PLAIN = fromString(TEXT_PLAIN_TYPE);
   public static final MediaType TEXT_CSS = fromString(TEXT_CSS_TYPE);
   public static final MediaType TEXT_CSV = fromString(TEXT_CSV_TYPE);
   public static final MediaType TEXT_HTML = fromString(TEXT_HTML_TYPE);
   public static final MediaType IMAGE_GIF = fromString(IMAGE_GIF_TYPE);
   public static final MediaType IMAGE_JPEG = fromString(IMAGE_JPEG_TYPE);
   public static final MediaType APPLICATION_PROTOSTUFF = fromString(APPLICATION_PROTOSTUFF_TYPE);
   public static final MediaType APPLICATION_KRYO = fromString(APPLICATION_KRYO_TYPE);
   /**
    * @deprecated Since 11.0, will be removed in 14.0. No longer used for BINARY storage.
    */
   @Deprecated
   public static final MediaType APPLICATION_INFINISPAN_BINARY = fromString(APPLICATION_INFINISPAN_BINARY_TYPE);
   public static final MediaType APPLICATION_PDF = fromString(APPLICATION_PDF_TYPE);
   public static final MediaType APPLICATION_RTF = fromString(APPLICATION_RTF_TYPE);
   public static final MediaType APPLICATION_ZIP = fromString(APPLICATION_ZIP_TYPE);
   /**
    * @deprecated Since 11.0, will be removed with ISPN-9622
    */
   @Deprecated
   public static final MediaType APPLICATION_INFINISPAN_MARSHALLING = fromString(APPLICATION_INFINISPAN_MARSHALLING_TYPE);
   /**
    * @deprecated Since 11.0, without replacement.
    */
   @Deprecated
   public static final MediaType APPLICATION_UNKNOWN = fromString(APPLICATION_UNKNOWN_TYPE);
   public static final MediaType MATCH_ALL = fromString(MATCH_ALL_TYPE);

   public static final String BYTE_ARRAY_TYPE = "ByteArray";
   private static final String INVALID_TOKENS = "()<>@,;:/[]?=\\\"";
   private static final String WEIGHT_PARAM_NAME = "q";
   private static final String CHARSET_PARAM_NAME = "charset";
   private static final String CLASS_TYPE_PARAM_NAME = "type";
   private static final double DEFAULT_WEIGHT = 1.0;
   private static final Charset DEFAULT_CHARSET = UTF_8;

   private final Map<String, String> params;
   private final String typeSubtype;
   private final int typeLength;
   private final boolean matchesAll;
   private final transient double weight;

   public MediaType(String type, String subtype) {
      this(type, subtype, emptyMap());
   }

   public MediaType(String type, String subtype, Map<String, String> params) {
      this(type + "/" + subtype, type.length(), params);
   }

   public MediaType(String typeSubtype) {
      this(typeSubtype, emptyMap());
   }

   public MediaType(String typeSubType, Map<String, String> params) {
      this(typeSubType, validate(typeSubType), params);
   }

   private MediaType(String typeSubType, int typeLength, Map<String, String> params) {
      this.typeSubtype = typeSubType;
      this.typeLength = typeLength;
      this.matchesAll = typeSubtype.equals(MATCH_ALL_TYPE);
      if (params != null) {
         this.params = Immutables.immutableMapCopy(params);
         String weight = params.get(WEIGHT_PARAM_NAME);
         this.weight = weight != null ? parseWeight(weight) : DEFAULT_WEIGHT;
      } else {
         this.params = emptyMap();
         this.weight = DEFAULT_WEIGHT;
      }
   }

   @ProtoField(number = 1)
   String getTree() {
      return toString();
   }

   /**
    * @deprecated replaced by {@link #fromString}
    */
   @Deprecated
   public static MediaType parse(String str) {
      return fromString(str);
   }

   @ProtoFactory
   public static MediaType fromString(String tree) {
      if (tree == null || tree.isEmpty()) throw CONTAINER.missingMediaType();

      Matcher matcher = TREE_PATTERN.matcher(tree);
      return parseSingleMediaType(tree, matcher, false);
   }

   private static MediaType parseSingleMediaType(String input, Matcher matcher, boolean isList) {
      if (!matcher.lookingAt() || matcher.start(1) < 0) {
         throw CONTAINER.invalidMediaTypeSubtype(input);
      }

      String typeSubtype;
      int typeLength;
      if (matcher.start(2) >= 0) {
         // group 1 is the full type+subtype, group 2 is the type
         typeSubtype = matcher.group(1);
         typeLength = matcher.end(2) - matcher.start(2);
      } else {
         // group 1 is "*"
         typeSubtype = MATCH_ALL_TYPE;
         typeLength = 1;
      }

      Map<String, String> paramMap = null;
      String firstParamName = null;
      String firstParamValue = null;
      while (matcher.end() < input.length()) {
         // find() doesn't skip any characters because of the \G
         if (!matcher.find()) {
            throw CONTAINER.invalidMediaTypeParam(input, input.substring(matcher.regionStart()));
         }

         // Groups 1 and 2 are only valid during the first match because of the ^
         String paramName = matcher.group(3);
         String paramValue = matcher.group(4);

         if (paramName == null) {
            // The comma alternative matched
            if (!isList) {
              throw CONTAINER.invalidMediaTypeSubtype(input);
            } else if (matcher.end() < input.length()) {
               // parseSingleMediaType will be called again
               break;
            } else {
               throw CONTAINER.invalidMediaTypeListCommaAtEnd(input);
            }
         }

         if (firstParamName == null) {
            firstParamName = paramName;
            firstParamValue = paramValue;
         } else {
            if (paramMap == null) {
               paramMap = new HashMap<>();
            }
            paramMap.put(firstParamName, firstParamValue);
            paramMap.put(paramName, paramValue);
         }
      }

      if (paramMap == null && firstParamName != null) {
         paramMap = Collections.singletonMap(firstParamName, firstParamValue);
      }

      return new MediaType(typeSubtype, typeLength, paramMap);
   }

   /**
    * Parse a comma separated list of media type trees.
    */
   public static Stream<MediaType> parseList(String mediaTypeList) {
      if (mediaTypeList == null || mediaTypeList.isEmpty()) throw CONTAINER.missingMediaType();

      Matcher matcher = TREE_PATTERN.matcher(mediaTypeList);
      List<MediaType> list = new ArrayList<>();
      while (true) {
         MediaType mediaType = parseSingleMediaType(mediaTypeList, matcher, true);
         list.add(mediaType);

         if (matcher.end() == mediaTypeList.length())
            break;

         matcher.region(matcher.end(), mediaTypeList.length());
      }

      list.sort(Comparator.comparingDouble(MediaType::getWeight).reversed());
      return list.stream();
   }

   private static double parseWeight(String weightValue) {
      try {
         return Double.parseDouble(weightValue);
      } catch (NumberFormatException nf) {
         throw CONTAINER.invalidWeight(weightValue);
      }
   }

   public boolean match(MediaType other) {
      if (other == this)
         return true;

      return other != null && (other.matchesAll() || this.matchesAll() || other.typeSubtype.equals(this.typeSubtype));
   }

   public boolean matchesAll() {
      return matchesAll;
   }

   public String getTypeSubtype() {
      return typeSubtype;
   }

   public MediaType withoutParameters() {
      if (params.isEmpty()) return this;
      return new MediaType(typeSubtype);
   }

   public double getWeight() {
      return weight;
   }

   public Charset getCharset() {
      return getParameter(CHARSET_PARAM_NAME).map(Charset::forName).orElse(DEFAULT_CHARSET);
   }

   public String getClassType() {
      return getParameter(CLASS_TYPE_PARAM_NAME).orElse(null);
   }

   public MediaType withClassType(Class<?> clazz) {
      return withParameter(CLASS_TYPE_PARAM_NAME, clazz.getName());
   }

   public boolean hasStringType() {
      String classType = getClassType();
      return classType != null && classType.equals(String.class.getName());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MediaType mediaType = (MediaType) o;
      return params.equals(mediaType.params) && typeSubtype.equals(mediaType.typeSubtype);
   }

   @Override
   public int hashCode() {
      int result = params.hashCode();
      result = 31 * result + typeSubtype.hashCode();
      return result;
   }

   public String getType() {
      return typeSubtype.substring(0, typeLength);
   }

   public String getSubType() {
      return typeSubtype.substring(typeLength + 1);
   }

   public boolean hasParameters() {
      return !params.isEmpty();
   }

   public Optional<String> getParameter(String name) {
      return Optional.ofNullable(params.get(name));
   }

   public Map<String, String> getParameters() {
      return Collections.unmodifiableMap(params);
   }

   public MediaType withParameters(Map<String, String> parameters) {
      return parameters.isEmpty() ? this : new MediaType(this.typeSubtype, parameters);
   }

   private static int validate(String typeSubtype) {
      if (typeSubtype == null) throw new NullPointerException("type and subtype cannot be null");
      Matcher matcher = TREE_PATTERN.matcher(typeSubtype);
      if (!matcher.matches())
         throw CONTAINER.invalidMediaTypeSubtype(typeSubtype);
      return matcher.end(2);
   }

   public MediaType withCharset(Charset charset) {
      return withParameter(CHARSET_PARAM_NAME, charset.toString());
   }

   public MediaType withParameter(String name, String value) {
      Map<String, String> newParams = new HashMap<>(params);
      newParams.put(name, value);
      return new MediaType(typeSubtype, newParams);
   }

   /**
    * @deprecated Use {@link #getParameters()} and {@link #getTypeSubtype()} to build a custom String representation.
    */
   @Deprecated
   public String toStringExcludingParam(String... params) {
      if (!hasParameters()) return typeSubtype;
      StringBuilder builder = new StringBuilder().append(typeSubtype);

      String strParams = this.params.entrySet().stream()
            .filter(e -> stream(params).noneMatch(p -> p.equals(e.getKey())))
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining("; "));

      if (strParams.isEmpty()) return builder.toString();
      return builder.append("; ").append(strParams).toString();
   }

   /**
    * @return true if the MediaType's java type is a byte array.
    */
   public boolean isBinary() {
      String customType = getClassType();
      if (customType == null) return !this.match(MediaType.APPLICATION_OBJECT);
      return BYTE_ARRAY_TYPE.equals(customType);
   }

   @Override
   public String toString() {
      if (!hasParameters()) return typeSubtype;
      StringBuilder builder = new StringBuilder().append(typeSubtype);

      String strParams = this.params.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining("; "));

      return builder.append("; ").append(strParams).toString();
   }

   public static final class MediaTypeExternalizer implements Externalizer<MediaType> {
      @Override
      public void writeObject(ObjectOutput output, MediaType mediaType) throws IOException {
         Short id = MediaTypeIds.getId(mediaType);
         if (id == null) {
            output.writeBoolean(false);
            output.writeUTF(mediaType.typeSubtype);
            output.writeObject(mediaType.params);
         } else {
            output.writeBoolean(true);
            output.writeShort(id);
            output.writeObject(mediaType.params);
         }
      }

      @Override
      @SuppressWarnings("unchecked")
      public MediaType readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         boolean isInternal = input.readBoolean();
         if (isInternal) {
            short id = input.readShort();
            Map<String, String> params = (Map<String, String>) input.readObject();
            return MediaTypeIds.getMediaType(id).withParameters(params);
         } else {
            String typeSubType = input.readUTF();
            Map<String, String> params = (Map<String, String>) input.readObject();
            return new MediaType(typeSubType, params);
         }
      }
   }

}
