package org.infinispan.configuration.cache;


import org.infinispan.interceptors.base.CommandInterceptor;

public class InterceptorConfiguration {
   
   public static enum Position {
      FIRST,LAST, OTHER_THAN_FIRST_OR_LAST
   }
   
   private final Class<? extends CommandInterceptor> after;
   private final Class<? extends CommandInterceptor> before;
   private final CommandInterceptor interceptor;
   private final int index;
   private final Position position;
   
   InterceptorConfiguration(Class<? extends CommandInterceptor> after, Class<? extends CommandInterceptor> before,
         CommandInterceptor interceptor, int index, Position position) {
      this.after = after;
      this.before = before;
      this.interceptor = interceptor;
      this.index = index;
      this.position = position;
   }
   
   public Class<? extends CommandInterceptor> after() {
      return after;
   }
   public Class<? extends CommandInterceptor> before() {
      return before;
   }
   public CommandInterceptor interceptor() {
      return interceptor;
   }
   public int index() {
      return index;
   }
   public Position position() {
      return position;
   }
   
   public boolean first() {
      return position() == Position.FIRST;
   }

   public boolean last() {
      return position() == Position.LAST;
   }

}
