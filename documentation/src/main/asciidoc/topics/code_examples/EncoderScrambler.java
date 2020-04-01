class Scrambler implements Encoder {

   public Object toStorage(Object content) {
   // Encrypt data
   }

   public Object fromStorage(Object content) {
   // Decrypt data
   }

   @Override
   public boolean isStorageFormatFilterable() {

   }

   public MediaType getStorageFormat() {
   return new MediaType("application", "scrambled");
   }

   @Override
   public short id() {
   //return id
   }
}
