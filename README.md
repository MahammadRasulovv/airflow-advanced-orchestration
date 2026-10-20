# Advanced Apache Airflow — MinIO, Mapping, Assets & Sensors

Praktiki Apache Airflow layihəsi: **Dynamic Task Mapping**, CSV/JSON branching, trigger rules, Asset-based Scheduling və DAG-lar arası asılılıqlar. Mühit: **Apache Airflow 3.1.0**, Docker Compose, **LocalExecutor**, MinIO və PostgreSQL 16.

> **İcra vəziyyəti:** 1, 2, 3, 4 və 6-cı tapşırıqlar Airflow UI-də test olunub. 5-ci tapşırıq üçün catchup qərarı və 3 günlük **backfill planı**, 7-ci tapşırıq üçün isə **deferrable/Triggerer nəzəri izahı** verilir. Backfill və deferrable rejiminin real icrası bu layihədə təsdiqlənməyib.

## Texnologiyalar

- Apache Airflow 3.1.0 (TaskFlow API / Standard və Amazon provider-ləri)
- Docker Compose, LocalExecutor
- MinIO (S3-compatible object storage)
- `S3Hook` və Airflow Connection: `minio_s3`
- PostgreSQL 16 (Airflow metadata bazası)

## DAG-lar və tapşırıqlar

| № | Mövzu | DAG | Nəticə |
|---|---|---|---|
| 1 | MinIO fayl siyahısı + Dynamic Task Mapping | `minio_dynamic_mapping` | Yoxlanılıb |
| 2 | CSV / JSON branching | `csv_json_branching` | Yoxlanılıb |
| 3 | Join və cleanup trigger rules | `csv_json_branching` | Yoxlanılıb; `all_done` üçün failure ssenarisi ayrıca test edilməyib |
| 4 | Producer/Consumer Asset event | `sales_producer`, `sales_consumer` | Yoxlanılıb |
| 5 | Catchup qərarı, 3 günlük backfill | `daily_sales` (nümunə / plan) | Planlaşdırılıb, icra təsdiqlənməyib |
| 6 | `ExternalTaskSensor` ilə `load_source` gözləməsi | `source_ingestion`, `source_waiter` | Yoxlanılıb |
| 7 | Deferrable rejimi və Triggerer | README izahı | Nəzəri izah; real Triggerer testi edilməyib |

## 1. Dynamic Task Mapping

`minio_dynamic_mapping` DAG-ında `discover_files` task-ı `S3Hook.list_keys()` ilə `advance-airflow` bucket-indəki obyekt adlarını qaytarır. `process_file.expand(file_name=discover_files())` hər fayl üçün ayrıca **mapped task instance** yaradır.

Test zamanı `customers.csv`, `orders.json`, `sales.csv` faylları üçün **3 mapped instance** uğurlu tamamlanıb. Bu mərhələdə task fayl formatını müəyyənləşdirib log yazır; tam data transformasiyası etmir.

```python
files = discover_files()
process_file.expand(file_name=files)
```

## 2. CSV / JSON Branching

`csv_json_branching` DAG-ı `get_file_list` vasitəsilə faylları MinIO-dan avtomatik alır, `@task.branch` ilə uyğun `process_csv` və `process_json` yollarını seçir. Bucket-də hər iki format varsa, hər iki yol işləyir. CSV task-ı `csv.DictReader`, JSON task-ı `json.loads` istifadə edir.

Bu DAG-da hər format üçün **bir task** var və uyğun fayllar həmin task daxilində dövr ilə işlənir; **hər fayla ayrıca mapped task** yaradılmır (mapping 1-ci DAG-da göstərilib).

## 3. Join və Cleanup — Trigger Rules

```python
@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def join():
    print("Selected branches completed successfully")

@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup():
    print("Cleanup executed")
```

- **`none_failed_min_one_success`**: JOIN-dən birbaşa əvvəl gələn task-lardan heç biri `failed`/`upstream_failed` olmamalı və ən azı biri `success` olmalıdır. Branching zamanı digər yol `skipped` olsa da JOIN işləyə bilər.
- **`all_done`**: CLEANUP-un birbaşa upstream task-ları terminal vəziyyətə çatdıqda, onların uğurlu olub-olmamasından asılı olmayaraq cleanup işə başlaya bilər. Bu qayda cleanup-ın özünün mütləq uğurlu olacağı demək deyil.

CSV/JSON seçim ssenarisi və JOIN-in `skipped` branch ilə işləməsi yoxlanılıb. Failure sonrası cleanup davranışı ayrıca simulyasiya edilməyib.

## 4. Producer / Consumer — Asset Scheduling

- **`sales_producer`**: `S3Hook.load_string()` ilə `advance-airflow/curated/sales.txt` faylını MinIO-ya yazır. `@task(outlets=[sales_asset])` sayəsində task uğurla bitəndə Airflow Asset update event yaradır.
- **`sales_consumer`**: `schedule=[sales_asset]` olduğuna görə event-dən sonra avtomatik DAG Run alır və MinIO-dakı faylı oxuyur.

Hər iki DAG eyni identifikatoru istifadə edir:

```python
sales_asset = Asset("s3://advance-airflow/curated/sales.txt")
```

**Vacib:** MinIO-ya faylın sadəcə xaricdən yüklənməsi Airflow Asset event yaratmır. Event producer task-ının `outlets` vasitəsilə uğurlu icrası ilə yaranır.

## 5. Catchup qərarı və 3 günlük Backfill planı

**Qərar: `catchup=False`.** DAG aktivləşdiriləndə bütün buraxılmış tarixi intervalların avtomatik işləməsini istəmirik. Lazım olan günləri ayrıca seçib backfill etmək daha nəzarətlidir. `catchup=False` manual backfill-i qadağan etmir.

Nümunə `daily_sales` DAG-ı gündəlik schedule ilə nəzərdə tutulur:

```python
with DAG(
    dag_id="daily_sales",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:
    ...
```

**Plan:** UTC üzrə **17, 18, 19 oktyabr 2026** tarixlərinin gündəlik data intervalları üçün üç ayrı run. `data_interval_start` və `data_interval_end` hər run-da uyğun dövrü göstərməlidir; real istehsal pipeline-ı bu tarixlərə məxsus data oxumalıdır.

Əvvəl sınaq:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id daily_sales --from-date 2026-10-17 --to-date 2026-10-19 --reprocess-behavior none --max-active-runs 1 --dry-run
```

Sonra (yoxlama uğurlu olduqda) icra:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id daily_sales --from-date 2026-10-17 --to-date 2026-10-19 --reprocess-behavior none --max-active-runs 1
```

`--reprocess-behavior none` eyni logical date üçün mövcud run-u təkrar yaratmır. `--max-active-runs 1` backfill run-larının paralelliyini məhdudlaşdırır. **Bu komandalar təqdim edilən plandır; layihədə uğurla işlədilməsi təsdiqlənməyib.** `schedule=None` olan DAG üçün bu tip time-based backfill uyğun deyil.

## 6. ExternalTaskSensor — DAG-lar arası asılılıq

Bu hissə iki DAG-dan ibarətdir:

1. `source_ingestion`: `trigger_waiter` ilə `source_waiter` DAG-ını avtomatik başladır, sonra `load_source` ilə MinIO-ya `source/source.txt` yazır.
2. `source_waiter`: `wait_for_ingestion` adlı `ExternalTaskSensor` eyni **logical date** üzrə `source_ingestion.load_source` task-ının `success` statusunu gözləyir; sonra `mart_build` task-ı faylı oxuyur.

Sensorun nüvəsi:

```python
wait_for_ingestion = ExternalTaskSensor(
    task_id="wait_for_ingestion",
    external_dag_id="source_ingestion",
    external_task_id="load_source",
    allowed_states=["success"],
    failed_states=["failed"],
    poke_interval=10,
    timeout=300,
    mode="reschedule",
    deferrable=False,
)
```

`TriggerDagRunOperator` istifadə edilməsinin səbəbi UI-dən yalnız `source_ingestion` DAG-ını manual başladarkən ikinci DAG-ın **eyni logical date** ilə avtomatik trigger edilməsidir. Bu mexanizm `ExternalTaskSensor`-u əvəz etmir: sensor yenə də başqa DAG-dakı task statusunu yoxlayır.

UI testində `trigger_waiter`, `load_source`, `wait_for_ingestion`, `mart_build` task-ları `success` olub. Sensor logunda `UP_FOR_RESCHEDULE` və sonradan `Success criteria met` mesajları görünür.

## 7. Deferrable Sensors və Triggerer — nəzəri izah

| Rejim | Gözləyərkən worker slotu | Tələb |
|---|---|---|
| `poke` | Gözləmə müddətində tutulur | Adi sensor icrası |
| `reschedule` | Yoxlamalar arasında buraxılır | Scheduler növbəti yoxlamanı planlaşdırır |
| `deferrable=True` | Task defer edildikdən sonra boşalır | **İşləyən Triggerer** və uyğun sensor/provider dəstəyi |

**Deferrable ardıcıllığı:** Worker task-ı başladır → task `deferred` olur və worker slotunu boşaldır → Triggerer gözləmə hadisəsini asinxron izləyir → şərt ödənəndə task worker-də davam edir.

Məsələn, uyğun provider dəstəkləyirsə:

```python
ExternalTaskSensor(
    task_id="wait_for_ingestion",
    external_dag_id="source_ingestion",
    external_task_id="load_source",
    deferrable=True,
)
```

`deferrable=True` üçün ayrıca **`airflow triggerer` prosesi** işləməlidir. Docker Compose-da bunun üçün `airflow-triggerer` servisi əlavə oluna bilər. Triggerer işləmirsə, deferred task normal şəkildə davam edə bilməz. Bizim **praktiki həll `mode="reschedule", deferrable=False`** ilə işləyir; buna görə mövcud test üçün Triggerer tələb olunmur.

## İşə salma və yoxlama

Layihənin Docker Compose qovluğunda:

```powershell
docker compose up -d
docker compose exec airflow-scheduler airflow version
docker compose exec airflow-scheduler airflow dags list-import-errors
```

- Airflow UI: `http://localhost:8081`
- MinIO UI: `http://localhost:9003`
- MinIO endpoint (Docker şəbəkəsi daxilindən): `http://minio:9000`
- MinIO bucket: `advance-airflow`
- Airflow Connection ID: `minio_s3` (`Amazon Web Services` tipli, MinIO endpoint-lə)

Connection credentials `.env` / lokal təhlükəsiz konfiqurasiyada saxlanmalıdır, README-yə və GitHub-a əlavə olunmamalıdır. `.env` faylını `.gitignore`-a daxil edin. Test üçün əvvəl MinIO bucket və faylları yaradılmalıdır.

### Ekran görüntüləri

Aşağıdakı adlarla `screenshots/` qovluğunda şəkillər yerləşdirilə bilər (mövcud olmayan fayllara qırıq image linki verməmək üçün sadəcə adlar göstərilib):

| Fayl | Göstərilən sübut |
|---|---|
| `01-dynamic-mapping.png` | 3 mapped instance, hamısı `success` |
| `02-branching.png` | CSV/JSON branch-ləri işləyir |
| `03-trigger-rules.png` | Skipped branch + uğurlu JOIN/CLEANUP |
| `04-sales-producer.png` | Producer task `success` |
| `05-sales-consumer.png` | Asset ilə işə düşmüş Consumer |
| `06-sales-asset.png` | Asset event və URI |
| `07-minio-sales.png` | `curated/sales.txt` MinIO-da |
| `08-source-ingestion.png` | `trigger_waiter` və `load_source` `success` |
| `09-external-sensor.png` | `wait_for_ingestion` və `mart_build` `success` |
| `10-sensor-logs.png` | Poking → reschedule → success |

## Yekun

Layihədə Airflow-un task mapping, branching, trigger rules, Asset Scheduling, `ExternalTaskSensor` və MinIO ilə Connection/Hook inteqrasiyası nümayiş etdirilir. **Catchup/backfill** və **deferrable/Triggerer** hissələri üçün nəzəri əsaslandırma və icra planı verilib; real test edilməyən nəticələr uğurlu icra kimi təqdim olunmur.

## Sənədlər

- [Apache Airflow — Backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html)
- [Apache Airflow — Deferrable Operators & Triggers](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
