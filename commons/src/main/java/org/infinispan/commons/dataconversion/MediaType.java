package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;


/**
 * Represent a content type with optional parameters.
 *
 * @since 9.2
 */
@SerializeWith(value = MediaType.MediaTypeExternalizer.class)
public final class MediaType {

   private static final Log log = LogFactory.getLog(MediaType.class);

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
   public static final String APPLICATION_UNKNOWN_TYPE = "application/unknown";
   public static final String WWW_FORM_URLENCODED_TYPE = "application/x-www-form-urlencoded";
   public static final String IMAGE_GIF_TYPE = "image/gif";
   public static final String IMAGE_JPEG_TYPE = "image/jpeg";
   public static final String IMAGE_PNG_TYPE = "image/png";
   public static final String TEXT_CSS_TYPE = "text/css";
   public static final String TEXT_CSV_TYPE = "text/csv";
   public static final String TEXT_PLAIN_TYPE = "text/plain";
   public static final String TEXT_HTML_TYPE = "text/html";
   public static final String APPLICATION_INFINISPAN_MARSHALLING_TYPE = "application/x-infinispan-marshalling";
   public static final String APPLICATION_INFINISPAN_BINARY_TYPE = "application/x-infinispan-binary";
   public static final String APPLICATION_PROTOSTUFF_TYPE = "application/x-protostuff";
   public static final String APPLICATION_KRYO_TYPE = "application/x-kryo";
   public static final String MATCH_ALL_TYPE = "*/*";

   public static MediaType APPLICATION_JSON = fromString(APPLICATION_JSON_TYPE);
   public static MediaType APPLICATION_OCTET_STREAM = fromString(APPLICATION_OCTET_STREAM_TYPE);
   public static MediaType APPLICATION_OBJECT = fromString(APPLICATION_OBJECT_TYPE);
   public static MediaType APPLICATION_SERIALIZED_OBJECT = fromString(APPLICATION_SERIALIZED_OBJECT_TYPE);
   public static MediaType APPLICATION_XML = fromString(APPLICATION_XML_TYPE);
   public static MediaType APPLICATION_PROTOSTREAM = fromString(APPLICATION_PROTOSTREAM_TYPE);
   public static MediaType APPLICATION_JBOSS_MARSHALLING = fromString(APPLICATION_JBOSS_MARSHALLING_TYPE);
   public static MediaType APPLICATION_INFINISPAN_MARSHALLED = fromString(APPLICATION_INFINISPAN_MARSHALLING_TYPE);
   public static MediaType APPLICATION_WWW_FORM_URLENCODED = fromString(WWW_FORM_URLENCODED_TYPE);
   public static MediaType IMAGE_PNG = fromString(IMAGE_PNG_TYPE);
   public static MediaType TEXT_PLAIN = fromString(TEXT_PLAIN_TYPE);
   public static MediaType TEXT_HTML = fromString(TEXT_HTML_TYPE);
   public static MediaType APPLICATION_PROTOSTUFF = fromString(APPLICATION_PROTOSTUFF_TYPE);
   public static MediaType APPLICATION_KRYO = fromString(APPLICATION_KRYO_TYPE);
   public static MediaType APPLICATION_INFINISPAN_BINARY = fromString(APPLICATION_INFINISPAN_BINARY_TYPE);
   public static MediaType APPLICATION_UNKNOWN = fromString(APPLICATION_UNKNOWN_TYPE);
   public static MediaType MATCH_ALL = fromString(MATCH_ALL_TYPE);

   private static final String INVALID_TOKENS = "()<>@,;:/[]?=\\\"";
   private static final String WEIGHT_PARAM_NAME = "q";
   private static final String CHARSET_PARAM_NAME = "charset";
   private static final double DEFAULT_WEIGHT = 1.0;
   private static final Charset DEFAULT_CHARSET = UTF_8;

   private final Map<String, String> params = new HashMap<>(2);
   private final String type;
   private final String subType;
   private final String typeSubtype;
   private final transient double weight;

   public MediaType(String type, String subtype) {
      this(type, subtype, emptyMap());
   }

   public MediaType(String type, String subtype, Map<String, String> params) {
      this.type = validate(type);
      this.subType = validate(subtype);
      this.typeSubtype = type + "/" + subtype;
      if (params != null) {
         this.params.putAll(params);
         String weight = params.get(WEIGHT_PARAM_NAME);
         this.weight = weight != null ? parseWeight(weight) : DEFAULT_WEIGHT;
      } else {
         this.weight = DEFAULT_WEIGHT;
      }
   }

   public static MediaType fromString(String mediaType) {
      return parse(mediaType);
   }

   public static MediaType parse(String str) {
      if (str == null || str.isEmpty()) throw log.missingMediaType();
      int separatorIdx = str.indexOf(';');
      boolean emptyParams = separatorIdx == -1;
      String types = emptyParams ? str : str.substring(0, separatorIdx);
      String params = emptyParams ? "" : str.substring(separatorIdx + 1);
      Map<String, String> paramMap = parseParams(params);

      // "*" is not a valid MediaType according to the https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html,
      // but we'll ignore for now to play nice with java.net.HttpURLConnection.
      // More details on https://bugs.openjdk.java.net/browse/JDK-8163921
      if (types.trim().equals("*")) {
         return emptyParams ? MediaType.MATCH_ALL : new MediaType("*", "*", paramMap);
      }
      if (types.indexOf('/') == -1) {
         throw log.invalidMediaTypeSubtype();
      }

      String[] typeSubtype = types.split("/");
      return new MediaType(typeSubtype[0].trim(), typeSubtype[1].trim(), paramMap);
   }

   public static Stream<MediaType> parseList(String mediaTypeList) {
      return stream(mediaTypeList.split(","))
            .map(MediaType::parse)
            .sorted(Comparator.comparingDouble((MediaType m) -> m.weight).reversed());
   }

   private static double parseWeight(String weightValue) {
      try {
         return Double.valueOf(weightValue);
      } catch (NumberFormatException nf) {
         throw log.invalidWeight(weightValue);
      }
   }

   private static Map<String, String> parseParams(String params) {
      Map<String, String> parsed = new HashMap<>();
      if (params == null || params.isEmpty()) return parsed;

      String[] parameters = params.split(";");

      for (String p : parameters) {
         if (!p.contains("=")) throw log.invalidMediaTypeParam(p);
         String[] nameValue = p.split("=");
         String paramName = nameValue[0].trim();
         String paramValue = nameValue[1].trim();
         boolean isQuoted = paramValue.startsWith("\"") || paramValue.startsWith("\'");
         String parsedValue = paramValue;
         if (isQuoted) {
            checkValidQuotes(paramValue);
            String quoted = nameValue[1].trim();
            parsedValue = quoted.substring(1, quoted.length() - 1);
         }
         parsed.put(validate(paramName), validate(parsedValue));
      }
      return parsed;
   }

   private static boolean checkStartAndEnd(String toCheck, char c) {
      return toCheck != null && toCheck.charAt(0) == c && toCheck.charAt(toCheck.length() - 1) == c;
   }

   private static void checkValidQuotes(String paramValue) {
      if (!checkStartAndEnd(paramValue, '\'') && !checkStartAndEnd(paramValue, '\"')) {
         throw log.unquotedMediaTypeParam();
      }
   }

   public boolean match(MediaType other) {
      return other != null && (other.matchesAll() || this.matchesAll() || (other.typeSubtype.equals(this.typeSubtype))
      );
   }

   public boolean matchesAll() {
      return this.typeSubtype.equals(MATCH_ALL_TYPE);
   }

   public String getTypeSubtype() {
      return typeSubtype;
   }

   public MediaType withoutParameters() {
      return new MediaType(type, subType);
   }

   public double getWeight() {
      return weight;
   }

   public Charset getCharset() {
      return getParameter(CHARSET_PARAM_NAME).map(Charset::forName).orElse(DEFAULT_CHARSET);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MediaType mediaType = (MediaType) o;
      return Objects.equals(params, mediaType.params) &&
            Objects.equals(type, mediaType.type) &&
            Objects.equals(subType, mediaType.subType);
   }

   @Override
   public int hashCode() {
      return Objects.hash(params, type, subType);
   }

   public String getType() {
      return type;
   }

   public String getSubType() {
      return subType;
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
      return new MediaType(this.type, this.subType, parameters);
   }

   private static String validate(String token) {
      if (token == null) throw new NullPointerException("type and subtype cannot be null");
      for (char c : token.toCharArray()) {
         if (c < 0x20 || c > 0x7F || INVALID_TOKENS.indexOf(c) > 0) {
            throw log.invalidCharMediaType(c, token);
         }
      }
      return token;
   }

   public MediaType withCharset(Charset charset) {
      return withParameter(CHARSET_PARAM_NAME, charset.toString());
   }

   public MediaType withParameter(String name, String value) {
      Map<String, String> newParams = new HashMap<>(params);
      newParams.put(name, value);
      return new MediaType(type, subType, newParams);
   }

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

   @Override
   public String toString() {
      return toStringExcludingParam(WEIGHT_PARAM_NAME);
   }

   public static final class MediaTypeExternalizer implements Externalizer<MediaType> {
      @Override
      public void writeObject(UserObjectOutput output, MediaType mediaType) throws IOException {
         String type = mediaType.type + "/" + mediaType.subType;
         Short id = MediaTypeIds.getId(type);
         if (id == null) {
            output.writeBoolean(false);
            output.writeUTF(mediaType.type);
            output.writeUTF(mediaType.subType);
            output.writeObject(mediaType.params);
         } else {
            output.writeBoolean(true);
            output.writeShort(id);
            output.writeObject(mediaType.params);
         }
      }

      @Override
      @SuppressWarnings("unchecked")
      public MediaType readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         boolean isInternal = input.readBoolean();
         if (isInternal) {
            short id = input.readShort();
            MediaType mediaType = MediaType.fromString(MediaTypeIds.getMediaType(id));
            Map<String, String> params = (Map<String, String>) input.readObject();
            params.forEach(mediaType.params::put);
            return mediaType;
         } else {
            String type = input.readUTF();
            String subType = input.readUTF();
            Map<String, String> params = (Map<String, String>) input.readObject();
            return new MediaType(type, subType, params);
         }
      }
   }

}
