public interface Handle<T extends CounterListener> {
   T getCounterListener();
   void remove();
}
