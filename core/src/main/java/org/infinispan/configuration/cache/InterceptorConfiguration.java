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
   
   public Class<? extends CommandInterceptor> getAfter() {
      return after;
   }
   public Class<? extends CommandInterceptor> getBefore() {
      return before;
   }
   public CommandInterceptor getInterceptor() {
      return interceptor;
   }
   public int getIndex() {
      return index;
   }
   public Position getPosition() {
      return position;
   }
   
   public boolean isFirst() {
      return getPosition() == Position.FIRST;
   }

   public boolean isLast() {
      return getPosition() == Position.LAST;
   }

}
