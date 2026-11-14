package lab;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** Açık TODO'lari dosya ve satır numarasıyla bulur. */
final class Todos {

    private Todos() {
    }

    static List<String> open(String lab) {
        List<String> found = new ArrayList<>();
        for (String directory : List.of("applications", "exercises")) {
            Path root = Env.ROOT.resolve(directory);
            if (!Files.isDirectory(root)) {
                continue;
            }
            try (Stream<Path> walk = Files.walk(root)) {
                walk.filter(Files::isRegularFile)
                        .filter(path -> !path.toString().contains("target"))
                        .filter(Todos::isSource)
                        .forEach(path -> scan(path, lab, found));
            } catch (IOException e) {
                throw new CheckFailed("Dosyalar taranamadı: " + e.getMessage());
            }
        }
        return found;
    }

    private static boolean isSource(Path path) {
        String name = path.getFileName().toString();
        return name.endsWith(".java") || name.endsWith(".yml") || name.endsWith(".avsc");
    }

    private static void scan(Path path, String lab, List<String> found) {
        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            for (int i = 0; i < lines.size(); i++) {
                int index = lines.get(i).indexOf("TODO " + lab + "-");
                if (index >= 0) {
                    String text = lines.get(i).substring(index);
                    String shortText = text.length() > 90 ? text.substring(0, 90) + "..." : text;
                    found.add(Env.ROOT.relativize(path) + ":" + (i + 1) + " " + shortText);
                }
            }
        } catch (IOException e) {
            throw new CheckFailed("Dosya okunamadı: " + path + ": " + e.getMessage());
        }
    }
}
