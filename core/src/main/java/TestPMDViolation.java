
public class TestPMDViolation {
    public void badMethod() {
        try {
            // This empty catch will trigger PMD violation
        } catch (Exception e) {
        }
    }
}
