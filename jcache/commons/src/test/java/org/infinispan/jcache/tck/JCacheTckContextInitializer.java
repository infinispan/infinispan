package org.infinispan.jcache.tck;

import java.io.IOException;

import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.SerializationContextInitializer;

import domain.Beagle;
import domain.Blog;
import domain.BorderCollie;
import domain.Chihuahua;
import domain.Dachshund;
import domain.Dog;
import domain.Identifier;
import domain.Papillon;
import domain.RoughCoatedCollie;
import domain.Sex;

/**
 * Provides marshallers for the jcache-tck so that it can be executed with ProtoStream.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class JCacheTckContextInitializer implements SerializationContextInitializer {

   private static String type(String message) {
      return String.format("org.infinispan.test.jcache.%s", message);
   }

   @Override
   public String getProtoFileName() {
      return "test.jcache.proto";
   }

   @Override
   public String getProtoFile() {
      return org.infinispan.protostream.FileDescriptorSource.getResourceAsString(getClass(), "/jcache.proto");
   }

   @Override
   public void registerSchema(org.infinispan.protostream.SerializationContext serCtx) {
      serCtx.registerProtoFiles(org.infinispan.protostream.FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
   }

   @Override
   public void registerMarshallers(org.infinispan.protostream.SerializationContext serCtx) {
      serCtx.registerMarshaller(new IdentityMarshaller());
      serCtx.registerMarshaller(new SexMarshaller());
      serCtx.registerMarshaller(new DogMarshaller());
      serCtx.registerMarshaller(new DogBreedMarshaller(Beagle.class));
      serCtx.registerMarshaller(new DogBreedMarshaller(BorderCollie.class));
      serCtx.registerMarshaller(new DogBreedMarshaller(Chihuahua.class));
      serCtx.registerMarshaller(new DogBreedMarshaller(Dachshund.class));
      serCtx.registerMarshaller(new DogBreedMarshaller(Papillon.class));
      serCtx.registerMarshaller(new DogBreedMarshaller(RoughCoatedCollie.class));
      serCtx.registerMarshaller(new BlogMarshaller());
   }

   static class IdentityMarshaller implements MessageMarshaller<Identifier> {
      @Override
      public Identifier readFrom(ProtoStreamReader reader) throws IOException {
         String name = reader.readString("name");
         return new Identifier(name);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, Identifier identifier) throws IOException {
         writer.writeString("name", identifier.toString());
      }

      @Override
      public Class<? extends Identifier> getJavaClass() {
         return Identifier.class;
      }

      @Override
      public String getTypeName() {
         return type("Identifier");
      }
   }

   static class SexMarshaller implements EnumMarshaller<Sex> {
      @Override
      public Sex decode(int enumValue) {
         return Sex.values()[enumValue];
      }

      @Override
      public int encode(Sex sex) throws IllegalArgumentException {
         return sex.ordinal();
      }

      @Override
      public Class<? extends Sex> getJavaClass() {
         return Sex.class;
      }

      @Override
      public String getTypeName() {
         return type("Sex");
      }
   }

   static class DogMarshaller implements MessageMarshaller<Dog> {
      @Override
      public Dog readFrom(ProtoStreamReader reader) throws IOException {
         Identifier name = reader.readObject("name", Identifier.class);
         String color = reader.readString("color");
         int weight = reader.readInt("weight");
         long length = reader.readLong("length");
         long height = reader.readLong("height");
         Sex sex = reader.readEnum("sex", Sex.class);
         boolean neutered = reader.readBoolean("neutered");
         return new Dog(name, color, weight, length, height, sex, neutered);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, Dog dog) throws IOException {
         writer.writeObject("name", dog.getName(), Identifier.class);
         writer.writeString("color", dog.getColor());
         writer.writeInt("weight", dog.getWeight());
         writer.writeLong("length", dog.getLengthInCm());
         writer.writeLong("height", dog.getHeight());
         writer.writeEnum("sex", dog.getSex());
         writer.writeBoolean("neutered", dog.isNeutered());
      }

      @Override
      public Class<? extends Dog> getJavaClass() {
         return Dog.class;
      }

      @Override
      public String getTypeName() {
         return type("Dog");
      }
   }

   static class DogBreedMarshaller implements MessageMarshaller<Dog> {

      private final Class<? extends Dog> clazz;

      DogBreedMarshaller(Class<? extends Dog> clazz) {
         this.clazz = clazz;
      }

      @Override
      public Dog readFrom(ProtoStreamReader reader) throws IOException {
         Dog delegate = reader.readObject("dog", Dog.class);
         try {
            Dog dog = clazz.newInstance();
            dog.setName(delegate.getName());
            dog.color(delegate.getColor());
            dog.weight(delegate.getWeight());
            dog.length(delegate.getLengthInCm());
            dog.height(delegate.getHeight());
            dog.sex(delegate.getSex());
            dog.neutered(delegate.isNeutered());
            return dog;
         } catch (Exception e) {
            throw new IllegalStateException(e);
         }
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, Dog dog) throws IOException {
         writer.writeObject("dog", dog, Dog.class);
      }

      @Override
      public Class<? extends Dog> getJavaClass() {
         return clazz;
      }

      @Override
      public String getTypeName() {
         return type(clazz.getSimpleName());
      }
   }

   static class BlogMarshaller implements MessageMarshaller<Blog> {

      @Override
      public Blog readFrom(ProtoStreamReader reader) throws IOException {
         try {
            String title = reader.readString("title");
            String body = reader.readString("body");
            return new Blog(title, body);
         } catch (Exception e) {
            throw new IllegalStateException(e);
         }
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, Blog blog) throws IOException {
         writer.writeString("title", blog.getTitle());
         writer.writeString("body", blog.getBody());
      }

      @Override
      public Class<? extends Blog> getJavaClass() {
         return Blog.class;
      }

      @Override
      public String getTypeName() {
         return type(Blog.class.getSimpleName());
      }
   }
}
