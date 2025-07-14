public class PmdViolationExample {

    public static void main(String[] args) {
        int unused = 42; // PMD: UnusedLocalVariable

        try {
            int result = 1 / 0;
        } catch (Exception e) {
            // PMD: EmptyCatchBlock
        }

        for (int i = 0; i < 10; i++) {
            // PMD: AvoidEmptyLoops
        }

        System.out.println("PMD test");
    }
}

