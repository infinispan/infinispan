package org.infinispan.scripting.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * ScriptMetadata. Holds meta information about a script obtained either implicitly by the script name and extension, or
 * explicitly by its header. See the "Script metadata" chapter in the User Guide for the syntax and format.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@ProtoTypeId(ProtoStreamTypeIds.SCRIPT_METADATA)
public class ScriptMetadata implements Metadata {

   private static final Set<String> TEXT_BASED_MEDIA = Util.asSet(TEXT_PLAIN_TYPE, APPLICATION_JSON_TYPE, APPLICATION_XML_TYPE);

   private final String name;

   private final ExecutionMode mode;

   private final String extension;

   private final Set<String> parameters;

   private final MediaType dataType;

   private final String language;

   private final String role;

   private final Map<String, String> properties;

   @ProtoFactory
   ScriptMetadata(String name, String language, String extension, ExecutionMode mode, Set<String> parameters,
                  String role, MediaType dataType, Map<String, String> properties) {
      this.name = Objects.requireNonNull(name);
      this.language = (language == null || language.isEmpty()) ? null : language;
      this.extension = extension;
      this.mode = Objects.requireNonNull(mode);
      this.parameters = Collections.unmodifiableSet(parameters);
      this.role = (role == null || role.isEmpty()) ? null : role;
      this.dataType = dataType;
      this.properties = properties == null ? Collections.emptyMap() : Map.copyOf(properties);
   }

   @ProtoField(number = 1)
   public String name() {
      return name;
   }

   @ProtoField(number = 2)
   public ExecutionMode mode() {
      return mode;
   }

   @ProtoField(number = 3)
   public String extension() {
      return extension;
   }

   @ProtoField(number = 4)
   public Set<String> parameters() {
      return parameters;
   }

   @ProtoField(number = 5)
   public MediaType dataType() {
      if (TEXT_BASED_MEDIA.contains(dataType.getTypeSubtype())) {
         return dataType.withClassType(String.class);
      }
      return dataType;
   }

   @ProtoField(number = 6)
   public Optional<String> language() {
      return Optional.ofNullable(language);
   }

   @ProtoField(number = 7)
   public Optional<String> role() {
      return Optional.ofNullable(role);
   }

   @ProtoField(number = 8)
   public Map<String, String> properties() {
      return properties;
   }

   @Override
   public long lifespan() {
      return -1;
   }

   @Override
   public long maxIdle() {
      return -1;
   }

   @Override
   public EntryVersion version() {
      return null;
   }

   @Override
   public Builder builder() {
      return new Builder()
            .name(name)
            .extension(extension)
            .mode(mode)
            .dataType(dataType)
            .language(language)
            .parameters(parameters)
            .role(role)
            .properties(properties);
   }

   @Override
   public String toString() {
      return "ScriptMetadata{" +
            "name='" + name + '\'' +
            ", mode=" + mode +
            ", extension='" + extension + '\'' +
            ", parameters=" + parameters +
            ", dataType=" + dataType +
            ", language=" + language +
            ", role=" + role +
            ", properties=" + properties +
            '}';
   }

   public static class Builder implements Metadata.Builder {
      private String name;
      private String extension;
      private String language;
      private String role;
      private ExecutionMode mode = ExecutionMode.LOCAL;
      private Set<String> parameters = Collections.emptySet();
      private MediaType dataType = MediaType.APPLICATION_OBJECT;
      private Map<String, String> properties;

      public ScriptMetadata.Builder name(String name) {
         this.name = name;
         return this;
      }

      public ScriptMetadata.Builder mode(ExecutionMode mode) {
         this.mode = mode;
         return this;
      }

      public ScriptMetadata.Builder extension(String extension) {
         this.extension = extension;
         return this;
      }

      public ScriptMetadata.Builder language(String language) {
         this.language = language;
         return this;
      }

      public ScriptMetadata.Builder parameters(Set<String> parameters) {
         this.parameters = parameters;
         return this;
      }

      public ScriptMetadata.Builder role(String role) {
         this.role = role;
         return this;
      }

      public ScriptMetadata.Builder dataType(MediaType dataType) {
         this.dataType = dataType;
         return this;
      }

      @Override
      public ScriptMetadata.Builder lifespan(long time, TimeUnit unit) {
         return this;
      }

      @Override
      public ScriptMetadata.Builder lifespan(long time) {
         return this;
      }

      @Override
      public ScriptMetadata.Builder maxIdle(long time, TimeUnit unit) {
         return this;
      }

      @Override
      public ScriptMetadata.Builder maxIdle(long time) {
         return this;
      }

      @Override
      public ScriptMetadata.Builder version(EntryVersion version) {
         return this;
      }

      public ScriptMetadata.Builder properties(Map<String, String> properties) {
         this.properties = properties;
         return this;
      }

      @Override
      public ScriptMetadata build() {
         return new ScriptMetadata(name, language, extension, mode, parameters, role, dataType, properties);
      }

      @Override
      public ScriptMetadata.Builder merge(Metadata metadata) {
         return this;
      }
   }
}
