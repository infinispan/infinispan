public static void main(String args[]) throws Exception {
   EmbeddedCacheManager manager = new DefaultCacheManager();
   manager.defineConfiguration("custom-cache", new ConfigurationBuilder()
             .eviction().strategy(LIRS).maxEntries(10)
             .build());
   Cache<Object, Object> c = manager.getCache("custom-cache");
}
