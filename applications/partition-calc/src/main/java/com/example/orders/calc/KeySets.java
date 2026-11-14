package com.example.orders.calc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Dört key seti. Hepsi sabit ve tekrarlanabilir: herkes aynı sayıları görür.
 * Her liste bir kaydı temsil eder; aynı key'in tekrarı o key'in trafiğidir.
 */
public final class KeySets {

    public static final int RECORDS = 30_000;

    private KeySets() {
    }

    /** Her kayıt farklı bir sipariş: yüksek çeşitlilik. */
    public static List<String> orderIds() {
        List<String> keys = new ArrayList<>(RECORDS);
        for (int i = 1; i <= RECORDS; i++) {
            keys.add(String.format("order-%06d", i));
        }
        return keys;
    }

    /** Az çeşit key, trafiğin çoğu tek ülkeden. */
    public static List<String> countries() {
        Map<String, Integer> percent = Map.of(
                "TR", 62, "DE", 14, "US", 8, "GB", 6, "NL", 5, "FR", 3, "IT", 2);
        List<String> keys = new ArrayList<>(RECORDS);
        percent.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            int count = RECORDS * entry.getValue() / 100;
            for (int i = 0; i < count; i++) {
                keys.add(entry.getKey());
            }
        });
        return keys;
    }

    /** 400 müşteri var ama tek büyük müşteri trafiğin %55'ini üretiyor. */
    public static List<String> bigCustomer() {
        List<String> keys = new ArrayList<>(RECORDS);
        int whale = RECORDS * 55 / 100;
        for (int i = 0; i < whale; i++) {
            keys.add("customer-0001");
        }
        for (int i = 0; i < RECORDS - whale; i++) {
            keys.add(String.format("customer-%04d", 2 + (i % 399)));
        }
        return keys;
    }

    /** Key'siz kayıtlar: null key. */
    public static List<String> keyless() {
        List<String> keys = new ArrayList<>(RECORDS);
        for (int i = 0; i < RECORDS; i++) {
            keys.add(null);
        }
        return keys;
    }
}
