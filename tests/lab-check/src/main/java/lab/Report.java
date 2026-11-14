package lab;

/** Check çıktısı. */
final class Report {

    private final String title;
    private int passed;

    Report(String title) {
        this.title = title;
        System.out.println();
        System.out.println("=== " + title + " ===");
    }

    void step(String text) {
        System.out.println();
        System.out.println("--- " + text);
    }

    void ok(String text) {
        passed++;
        System.out.println("  [TAMAM] " + text);
    }

    void info(String text) {
        System.out.println("  [BİLGİ] " + text);
    }

    void warn(String text) {
        System.out.println("  [UYARI] " + text);
    }

    int success() {
        System.out.println();
        System.out.println("SONUÇ: BAŞARILI · " + title + " · " + passed + " kontrol geçti");
        return 0;
    }

    int failure(CheckFailed failure) {
        System.out.println("  [HATA]  " + failure.getMessage());
        if (failure.hint() != null) {
            System.out.println("          Ne yapmalı: " + failure.hint());
        }
        System.out.println();
        System.out.println("SONUÇ: BAŞARISIZ · " + title + " · " + passed + " kontrol geçti, ilk hatada duruldu");
        return 1;
    }
}
