#!/usr/bin/env bash
# Lab komutları. Tüm mantık tests/lab-check içinde; lab.ps1 aynı aracı çalıştırır.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if ! command -v java >/dev/null 2>&1; then
  echo "[HATA] Java bulunamadi. Java 17 kur, sonra tekrar dene." >&2
  exit 1
fi
major="$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)"
if [ "${major:-0}" -lt 17 ]; then
  echo "[HATA] Java 17 veya üzeri gerekli. Bulunan: ${major:-bilinmiyor}" >&2
  exit 1
fi

JAR="tests/lab-check/target/lab-check.jar"
if [ ! -f "$JAR" ] || [ -n "$(find tests/lab-check/src tests/lab-check/pom.xml -newer "$JAR" -print -quit)" ]; then
  echo "lab-check derleniyor..."
  ./mvnw -q -DskipTests -pl tests/lab-check package
fi

exec java -jar "$JAR" "$@"
