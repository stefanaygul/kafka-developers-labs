package lab;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** lab.sh ve lab.ps1 buraya gelir: tüm mantık tek yerde, tüm işletim sistemlerinde aynıdır. */
public final class Main {

    private static final String USAGE = """
            Kullanım: ./lab.sh <komut> [lab] [seçenekler]

              up <LAB>                   Lab ortamını başlatır (veriyi silmez)
              up optional                ksqlDB ve Kafka Connect demolarını başlatır
              check <LAB>                Lab'i gerçek davranışıyla doğrular
              down                       Container'ları durdurur (veri kalır)
              solution <LAB>             Çözüm farkını gösterir (dosyalara dokunmaz)
              solution <LAB> --apply     Dosyalarını yedekleyip çözümü uygular
              restore <LAB>              En son yedeği geri yükler
              status                     Container, broker ve topic durumu
              run <uygulama> [arg...]    Uygulamayı çalıştırır
              probe                      Host'tan her partition'a yazmayı dener
              emit customers|late-events|poison [topic]
              measure rebalance          Rebalance protokollerini ölçer
              incident start|verify      D2-06 olay sayımı
              reset --destructive-reset  TÜM lab verisini siler

            Lab'ler: D1-01 D1-02 D1-03 D1-04 D2-01 D2-02 D2-03 D2-04 D2-05 D2-06 D3-01 D3-02 D3-03
            """;

    private Main() {
    }

    public static void main(String[] args) {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        int code;
        try {
            code = dispatch(args);
        } catch (CheckFailed failure) {
            System.out.println("[HATA] " + failure.getMessage());
            if (failure.hint() != null) {
                System.out.println("       Ne yapmalı: " + failure.hint());
            }
            code = 1;
        } catch (Exception e) {
            System.out.println("[HATA] Beklenmeyen hata: " + e);
            code = 1;
        }
        System.exit(code);
    }

    private static int dispatch(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.print(USAGE);
            return 2;
        }
        List<String> rest = new ArrayList<>(Arrays.asList(args).subList(1, args.length));
        switch (args[0]) {
            case "up":
                if ("optional".equals(required(rest, "lab"))) {
                    return Infra.upOptional();
                }
                return Infra.up(Labs.get(required(rest, "lab")), rest.contains("--broken"), rest.contains("--single"));
            case "down":
                return Infra.down();
            case "status":
                return Infra.status();
            case "check":
                return Checks.run(required(rest, "lab"));
            case "solution":
                String lab = Labs.get(required(rest, "lab")).id();
                return rest.contains("--apply") ? Solutions.apply(lab) : Solutions.show(lab);
            case "restore":
                return Solutions.restore(Labs.get(required(rest, "lab")).id());
            case "run":
                return run(rest);
            case "reset":
                return Infra.reset(rest.contains("--destructive-reset"));
            case "probe":
                return Emit.probe();
            case "emit":
                return Emit.command(rest);
            case "measure":
                return Rebalance.measure();
            case "incident":
                return Incident.command(required(rest, "start|verify"));
            default:
                System.out.print(USAGE);
                return 2;
        }
    }

    private static int run(List<String> args) {
        String app = required(args, "uygulama");
        if (!java.nio.file.Files.isDirectory(Env.ROOT.resolve("applications/" + app))) {
            throw new CheckFailed("Bilinmeyen uygulama: " + app,
                    "Seçenekler: partition-calc, order-producer, order-consumer, order-processor, order-streams, order-avro");
        }
        List<String> mvn = new ArrayList<>(List.of("-q", "-pl", "applications/" + app, "spring-boot:run"));
        List<String> appArgs = args.subList(1, args.size());
        if (!appArgs.isEmpty()) {
            mvn.add("-Dspring-boot.run.arguments=" + String.join(" ", appArgs));
        }
        return Shell.inherit(Shell.mvnw(mvn.toArray(String[]::new)));
    }

    private static String required(List<String> args, String name) {
        return args.stream().filter(arg -> !arg.startsWith("--")).findFirst()
                .orElseThrow(() -> new CheckFailed("Eksik argüman: " + name, "Kullanım için: ./lab.sh"));
    }
}
