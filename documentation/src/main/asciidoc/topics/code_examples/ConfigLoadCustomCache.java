public static void main(String args[]) throws Exception {
	Cache<Object, Object> c = new DefaultCacheManager("infinispan.xml").getCache("xml-configured-cache");
}
