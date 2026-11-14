package lab;

/** Beklenen davranış görülmedi. Mesaj tam olarak neyin eksik olduğunu söyler. */
final class CheckFailed extends RuntimeException {

    private final String hint;

    CheckFailed(String message, String hint) {
        super(message);
        this.hint = hint;
    }

    CheckFailed(String message) {
        this(message, null);
    }

    String hint() {
        return hint;
    }
}
