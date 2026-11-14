package lab;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Çözümler solutions/LAB altında ayrı durur. Varsayılan komut yalnızca farkı gösterir,
 * dosyalara dokunmaz. --apply önce öğrenenin dosyalarını yedekler.
 */
final class Solutions {

    private Solutions() {
    }

    private static Path root(String lab) {
        return Env.ROOT.resolve("solutions").resolve(lab);
    }

    static List<Path> files(String lab) {
        Path root = root(lab);
        if (!Files.isDirectory(root)) {
            return List.of();
        }
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).map(root::relativize).sorted().toList();
        } catch (IOException e) {
            throw new CheckFailed("Çözüm dosyaları okunamadı: " + e.getMessage());
        }
    }

    static int show(String lab) throws IOException {
        if (CliSolutions.has(lab)) {
            CliSolutions.show(lab);
            System.out.println();
        }
        List<Path> files = files(lab);
        if (files.isEmpty()) {
            return 0;
        }
        System.out.println("Dosyalarına dokunulmadı. Sadece fark gösteriliyor.");
        for (Path file : files) {
            System.out.println();
            System.out.println("##### " + file);
            Path current = Env.ROOT.resolve(file);
            List<String> mine = Files.exists(current) ? Files.readAllLines(current) : List.of();
            printDiff(mine, Files.readAllLines(root(lab).resolve(file)));
        }
        System.out.println();
        System.out.println("Uygulamak için: ./lab.sh solution " + lab + " --apply (önce dosyalarını yedekler)");
        return 0;
    }

    static int apply(String lab) throws IOException {
        if (CliSolutions.has(lab)) {
            CliSolutions.apply(lab);
        }
        Path backup = Env.ROOT.resolve(".lab-backup").resolve(lab + "-"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS")));
        for (Path file : files(lab)) {
            Path current = Env.ROOT.resolve(file);
            if (Files.exists(current)) {
                Path target = backup.resolve(file);
                Files.createDirectories(target.getParent());
                Files.copy(current, target, StandardCopyOption.REPLACE_EXISTING);
            }
            Files.createDirectories(current.getParent());
            Files.copy(root(lab).resolve(file), current, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("Uygulandı: " + file);
        }
        if (Files.isDirectory(backup)) {
            System.out.println("Yedek: " + Env.ROOT.relativize(backup));
            System.out.println("Geri almak için: ./lab.sh restore " + lab);
        }
        return 0;
    }

    static int restore(String lab) throws IOException {
        Path backups = Env.ROOT.resolve(".lab-backup");
        if (!Files.isDirectory(backups)) {
            throw new CheckFailed(lab + " için yedek yok.");
        }
        Path latest;
        try (Stream<Path> list = Files.list(backups)) {
            latest = list.filter(path -> path.getFileName().toString().startsWith(lab + "-"))
                    .max(Comparator.comparing(path -> path.getFileName().toString()))
                    .orElseThrow(() -> new CheckFailed(lab + " için yedek yok."));
        }
        try (Stream<Path> walk = Files.walk(latest)) {
            for (Path file : walk.filter(Files::isRegularFile).toList()) {
                Path relative = latest.relativize(file);
                Files.copy(file, Env.ROOT.resolve(relative), StandardCopyOption.REPLACE_EXISTING);
                System.out.println("Geri yüklendi: " + relative);
            }
        }
        return 0;
    }

    /** Küçük dosyalar için LCS tabanlı satır farkı. Git kurulu olması gerekmez. */
    private static void printDiff(List<String> before, List<String> after) {
        int[][] lcs = new int[before.size() + 1][after.size() + 1];
        for (int i = before.size() - 1; i >= 0; i--) {
            for (int j = after.size() - 1; j >= 0; j--) {
                lcs[i][j] = before.get(i).equals(after.get(j))
                        ? lcs[i + 1][j + 1] + 1
                        : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
            }
        }
        List<String> changes = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < before.size() || j < after.size()) {
            if (i < before.size() && j < after.size() && before.get(i).equals(after.get(j))) {
                i++;
                j++;
            } else if (j < after.size() && (i == before.size() || lcs[i][j + 1] >= lcs[i + 1][j])) {
                changes.add("+ " + after.get(j++));
            } else {
                changes.add("- " + before.get(i++));
            }
        }
        if (changes.isEmpty()) {
            System.out.println("(fark yok)");
        }
        changes.forEach(System.out::println);
    }
}
