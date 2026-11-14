package com.example.orders.calc;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Komut satırı ve histogram. Bu dosyayı değiştirmen gerekmiyor. */
public final class PartitionCalc {

    private static final int[] PARTITION_COUNTS = {3, 6, 12};
    private static final int BAR_WIDTH = 40;
    private static final double HOT_THRESHOLD = 1.5;

    private PartitionCalc() {
    }

    public static void main(String[] args) {
        Map<String, String> options = parse(args);
        if (options.containsKey("key")) {
            int partitions = Integer.parseInt(options.getOrDefault("partitions", "6"));
            System.out.println("partition=" + PartitionMath.partitionFor(options.get("key"), partitions));
            return;
        }
        Map<String, List<String>> sets = new LinkedHashMap<>();
        sets.put("order-id", KeySets.orderIds());
        sets.put("country", KeySets.countries());
        sets.put("big-customer", KeySets.bigCustomer());
        sets.put("keyless", KeySets.keyless());

        if (options.containsKey("machine")) {
            printMachineReadable(sets);
            return;
        }
        String only = options.get("set");
        int[] counts = options.containsKey("partitions")
                ? new int[] {Integer.parseInt(options.get("partitions"))}
                : PARTITION_COUNTS;
        for (Map.Entry<String, List<String>> set : sets.entrySet()) {
            if (only != null && !only.equals(set.getKey())) {
                continue;
            }
            for (int partitions : counts) {
                printHistogram(set.getKey(), set.getValue(), partitions);
            }
            printMoved(set.getKey(), set.getValue());
        }
    }

    private static void printHistogram(String name, List<String> keys, int partitions) {
        int[] counts = PartitionMath.histogram(keys, partitions);
        long total = Arrays.stream(counts).sum();
        long distinct = keys.stream().filter(k -> k != null).distinct().count();
        double average = (double) keys.size() / partitions;
        int max = Arrays.stream(counts).max().orElse(0);
        System.out.printf("%n=== Set: %s | partition: %d | kayit: %d | farkli key: %d ===%n",
                name, partitions, total, distinct);
        for (int p = 0; p < partitions; p++) {
            int bar = max == 0 ? 0 : (int) Math.round((double) counts[p] / max * BAR_WIDTH);
            String mark = counts[p] >= average * HOT_THRESHOLD ? "  <-- HOT" : "";
            System.out.printf("P%-2d %-40s %6d (%4.1f%%)%s%n", p, "#".repeat(bar), counts[p],
                    keys.isEmpty() ? 0 : 100.0 * counts[p] / keys.size(), mark);
        }
        System.out.printf("En yoğun partition ortalamanın %.1f katı%n", average == 0 ? 0 : max / average);
    }

    private static void printMoved(String name, List<String> keys) {
        List<String> distinct = keys.stream().filter(k -> k != null)
                .collect(java.util.stream.Collectors.collectingAndThen(
                        java.util.stream.Collectors.toCollection(LinkedHashSet::new), List::copyOf));
        if (distinct.isEmpty()) {
            System.out.println("\nKey'siz kayıtlarda key taşınması yoktur: sıralama zaten garanti edilmez.");
            return;
        }
        System.out.printf("%nPartition sayisi degisince yer degistiren key orani (%s):%n", name);
        System.out.printf("  3 -> 6  : %%%.1f%n", PartitionMath.movedKeyPercent(distinct, 3, 6));
        System.out.printf("  6 -> 12 : %%%.1f%n", PartitionMath.movedKeyPercent(distinct, 6, 12));
    }

    private static void printMachineReadable(Map<String, List<String>> sets) {
        for (Map.Entry<String, List<String>> set : sets.entrySet()) {
            for (int partitions : PARTITION_COUNTS) {
                System.out.println("histogram " + set.getKey() + " " + partitions + " "
                        + Arrays.toString(PartitionMath.histogram(set.getValue(), partitions)));
            }
        }
        System.out.printf(java.util.Locale.ROOT, "moved order-id 3 6 %.1f%n",
                PartitionMath.movedKeyPercent(KeySets.orderIds(), 3, 6));
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String name = args[i].substring(2);
                boolean hasValue = i + 1 < args.length && !args[i + 1].startsWith("--");
                options.put(name, hasValue ? args[++i] : "true");
            }
        }
        return options;
    }
}
