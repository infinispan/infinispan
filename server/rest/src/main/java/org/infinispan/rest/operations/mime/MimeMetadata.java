package org.infinispan.rest.operations.mime;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.Metadata;
import org.jboss.marshalling.util.IdentityIntMap;

/**
 * Metadata for MIME data stored in REST servers.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MimeMetadata implements Metadata {
   protected final String contentType;
   protected final InvocationRecord records;

   public MimeMetadata(String contentType, InvocationRecord records) {
      this.contentType = Objects.requireNonNull(contentType);
      this.records = records;
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
   public InvocationRecord lastInvocation() {
      return records;
   }

   @Override
   public InvocationRecord invocation(CommandInvocationId id) {
      return InvocationRecord.lookup(records, id);
   }

   @Override
   public Builder builder() {
      return new MimeMetadataBuilder().contentType(contentType).invocations(records);
   }

   public String contentType() {
      return contentType;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MimeMetadata that = (MimeMetadata) o;

      return contentType.equals(that.contentType);

   }

   @Override
   public int hashCode() {
      return 41 + contentType.hashCode();
   }

   @Override
   public String toString() {
      return "MimeMetadata{" +
            "contentType='" + contentType + '\'' +
            '}';
   }

   private static final int Immortal = 0;
   private static final int Expirable = 1;
   private static final int LifespanExpirable = 2;
   private static final int MaxIdleExpirable = 3;

   private static final IdentityIntMap<Class> numbers = new IdentityIntMap<>(4);

   static {
      numbers.put(MimeMetadata.class, Immortal);
      numbers.put(MimeExpirableMetadata.class, Expirable);
      numbers.put(MimeLifespanExpirableMetadata.class, LifespanExpirable);
      numbers.put(MimeMaxIdleExpirableMetadata.class, MaxIdleExpirable);
   }

   public static class Externalizer extends AbstractExternalizer<MimeMetadata> {

      @Override
      public MimeMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String contentType = input.readUTF();
         InvocationRecord records = InvocationRecord.readListFrom(input);
         int number = input.readUnsignedByte();
         switch (number) {
            case Immortal:
               return new MimeMetadata(contentType, records);
            case Expirable:
               long lifespan = input.readLong();
               long maxIdle = input.readLong();
               return new MimeExpirableMetadata(contentType, records, lifespan, TimeUnit.MILLISECONDS, maxIdle, TimeUnit.MILLISECONDS);
            case LifespanExpirable:
               lifespan = input.readLong();
               return new MimeLifespanExpirableMetadata(contentType, records, lifespan, TimeUnit.MILLISECONDS);
            case MaxIdleExpirable:
               maxIdle = input.readLong();
               return new MimeMaxIdleExpirableMetadata(contentType, records, maxIdle, TimeUnit.MILLISECONDS);
            default:
               throw new IllegalArgumentException("Unsupported type: " + number);
         }
      }

      @Override
      public void writeObject(ObjectOutput output, MimeMetadata meta) throws IOException {
         output.writeUTF(meta.contentType);
         InvocationRecord.writeListTo(output, meta.records);
         int number = numbers.get(meta.getClass(), -1);
         output.write(number);
         switch (number) {
            case Immortal:
               // no-op
               break;
            case Expirable:
               output.writeLong(meta.lifespan());
               output.writeLong(meta.maxIdle());
               break;
            case LifespanExpirable:
               output.writeLong(meta.lifespan());
               break;
            case MaxIdleExpirable:
               output.writeLong(meta.maxIdle());
               break;
            default:
               throw new IllegalArgumentException("Unsupported type: " + number);
         }
      }

      @Override
      public Set<Class<? extends MimeMetadata>> getTypeClasses() {
         return Util.asSet(MimeMetadata.class, MimeExpirableMetadata.class,
               MimeLifespanExpirableMetadata.class, MimeMaxIdleExpirableMetadata.class);
      }
   }
}

class MimeExpirableMetadata extends MimeMetadata {
   private final long lifespanTime;
   private final TimeUnit lifespanUnit;
   private final long maxIdleTime;
   private final TimeUnit maxIdleUnit;

   public MimeExpirableMetadata(String contentType, InvocationRecord records, long lifespanTime, TimeUnit lifespanUnit, long maxIdleTime,
                                TimeUnit maxIdleUnit) {
      super(contentType, records);
      this.lifespanTime = lifespanTime;
      this.lifespanUnit = Objects.requireNonNull(lifespanUnit);
      this.maxIdleTime = maxIdleTime;
      this.maxIdleUnit = Objects.requireNonNull(maxIdleUnit);
   }

   @Override
   public long lifespan() {
      return lifespanUnit.toMillis(lifespanTime);
   }

   @Override
   public long maxIdle() {
      return maxIdleUnit.toMillis(maxIdleTime);
   }

   @Override
   public Builder builder() {
      return super.builder().lifespan(lifespanTime, lifespanUnit).maxIdle(maxIdleTime, maxIdleUnit);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      MimeExpirableMetadata that = (MimeExpirableMetadata) o;

      if (lifespanTime != that.lifespanTime) return false;
      if (maxIdleTime != that.maxIdleTime) return false;
      if (lifespanUnit != that.lifespanUnit) return false;
      return maxIdleUnit == that.maxIdleUnit;

   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (lifespanTime ^ (lifespanTime >>> 32));
      result = 31 * result + lifespanUnit.hashCode();
      result = 31 * result + (int) (maxIdleTime ^ (maxIdleTime >>> 32));
      result = 31 * result + maxIdleUnit.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "MimeExpirableMetadata{" +
            "lifespanTime=" + lifespanTime +
            ", lifespanUnit=" + lifespanUnit +
            ", maxIdleTime=" + maxIdleTime +
            ", maxIdleUnit=" + maxIdleUnit +
            '}';
   }
}

class MimeLifespanExpirableMetadata extends MimeMetadata {
   private final long lifespanTime;
   private final TimeUnit lifespanUnit;

   MimeLifespanExpirableMetadata(String contentType, InvocationRecord records, long lifespanTime, TimeUnit lifespanUnit) {
      super(contentType, records);
      this.lifespanTime = lifespanTime;
      this.lifespanUnit = Objects.requireNonNull(lifespanUnit);
   }

   @Override
   public long lifespan() {
      return lifespanUnit.toMillis(lifespanTime);
   }

   @Override
   public Builder builder() {
      return super.builder().lifespan(lifespanTime, lifespanUnit);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      MimeLifespanExpirableMetadata that = (MimeLifespanExpirableMetadata) o;

      if (lifespanTime != that.lifespanTime) return false;
      return lifespanUnit == that.lifespanUnit;

   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (lifespanTime ^ (lifespanTime >>> 32));
      result = 31 * result + lifespanUnit.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "MimeLifespanExpirableMetadata{" +
            "lifespanTime=" + lifespanTime +
            ", lifespanUnit=" + lifespanUnit +
            '}';
   }
}

class MimeMaxIdleExpirableMetadata extends MimeMetadata {
   private final long maxIdleTime;
   private final TimeUnit maxIdleUnit;

   public MimeMaxIdleExpirableMetadata(String contentType, InvocationRecord records, long maxIdleTime, TimeUnit maxIdleUnit) {
      super(contentType, records);
      this.maxIdleTime = maxIdleTime;
      this.maxIdleUnit = Objects.requireNonNull(maxIdleUnit);
   }

   @Override
   public long maxIdle() {
      return maxIdleUnit.toMillis(maxIdleTime);
   }

   @Override
   public Builder builder() {
      return super.builder().maxIdle(maxIdleTime, maxIdleUnit);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      MimeMaxIdleExpirableMetadata that = (MimeMaxIdleExpirableMetadata) o;

      if (maxIdleTime != that.maxIdleTime) return false;
      return maxIdleUnit == that.maxIdleUnit;

   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (maxIdleTime ^ (maxIdleTime >>> 32));
      result = 31 * result + maxIdleUnit.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "MimeMaxIdleExpirableMetadata{" +
            "maxIdleTime=" + maxIdleTime +
            ", maxIdleUnit=" + maxIdleUnit +
            '}';
   }
}
