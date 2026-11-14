# Başlangıç

Eğitimden önce bu sayfayı bitir. Yaklaşık 15 dakika sürer ve imajlar için internet gerekir.

## Ön koşullar

| Gerekli | Kontrol | Beklenen |
|---|---|---|
| Java 17 veya üzeri | `java -version` | `17` ya da üzeri |
| Docker Desktop | `docker info` | Hatasız çıktı |
| 16 GB RAM, 20 GB boş disk | | Docker'a en az 6 GB bellek ver |
| IDE | IntelliJ IDEA ya da başka bir IDE | |

Maven kurmana gerek yok: repo `mvnw` ile gelir.

## Komutlar

Her şey repo kökünden çalışır.

| macOS, Linux | Windows PowerShell |
|---|---|
| `./lab.sh up D1-01` | `.\lab.ps1 up D1-01` |

Windows'ta `curl` yerine `curl.exe` yaz.

Yalnızca dört komut bilmen yeterli:

| Komut | Ne yapar |
|---|---|
| `./lab.sh up <LAB>` | Lab ortamını başlatır, veriyi silmez |
| `./lab.sh check <LAB>` | Lab'i gerçek davranışıyla doğrular, eksiği söyler |
| `./lab.sh solution <LAB>` | Çözüm farkını gösterir, dosyalarına dokunmaz |
| `./lab.sh status` | Ortamın durumunu gösterir |

## Ortam kontrolü

```bash
./lab.sh up D1-01
./lab.sh status
```

Beklenen çıktı:

```text
Cluster hazır: 3 broker, aktif controller 2
Broker 1: localhost:9092
Broker 2: localhost:9094
Broker 3: localhost:9096
```

Sonra imajları ve bağımlılıkları önceden indir; sınıfta interneti beklemezsin:

```bash
./lab.sh up D3-02
./lab.sh up D2-03
./mvnw -q -DskipTests package
```

Port dolu hatası alırsan araç hangi sürecin tuttuğunu söyler. Ayrıntı: [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)

## Şimdi ne yapacaksın?

Her lab'in kendi README'si var. Başka hiçbir şey okumana gerek yok. İlk lab: [D1-01](exercises/day1/D1-01-ilk-mesaj/README.md)
