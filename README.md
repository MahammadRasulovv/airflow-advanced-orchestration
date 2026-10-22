# Advanced Airflow Orchestration — Producer & Consumer

**Apache Airflow 3.1.0 · Docker Compose · LocalExecutor · MinIO · S3Hook · Asset Scheduling**

Bu layihə müəllimin verdiyi **7 tələbi cəmi 2 DAG** daxilində nümayiş etdirir. `producer` DAG-ı ilk dörd praktiki tələbi icra edir; `consumer` isə Asset event ilə avtomatik başlayır və başqa DAG-dakı `load_source` task-ını `ExternalTaskSensor` ilə gözləyir. **5-ci tapşırıq üçün backfill planı, 7-ci üçün deferrable/Triggerer izahı sənədləşdirilib**, amma bu iki hissənin ayrıca praktik icrası göstərilmir.

> **Test sübutu:** 22 oktyabr 2026, son uğurlu `producer` run təxminən 17:46:02 (Bakı vaxtı), `consumer` Asset-triggered run isə 17:46:12. Ekran görüntülərində görünən **əvvəlki uğursuz Consumer run** əvvəlki kod versiyasına aiddir; son run uğurludur.

## Arxitektura və texnologiyalar

```text
                          PRODUCER (manual Trigger)
load_source → discover_files → process_file.expand() → select_branch
                                                     ├─ process_csv ─┐
                                                     └─ process_json ┤
                                                                    ↓
                                                                   join
                                                                    ↓
                                                                 cleanup
                                                                    ↓
                                                               publish_sales
                                                                    │
                                             sales Asset update event
                                                                    ↓
                                    CONSUMER (asset-triggered, automatic)
                                     wait_for_ingestion → mart_build
                                     ExternalTaskSensor    S3Hook.read_key
```

- Airflow: **3.1.0**, `LocalExecutor`, PostgreSQL 16 metadata DB
- Object storage: **MinIO**, bucket `advance-airflow`, Connection ID `minio_s3`
- Asset URI: `s3://advance-airflow/curated/sales.csv`
- İcra qaydası: Airflow UI-də yalnız **`producer`** manual trigger edilir; `consumer` yeni Asset event ilə işə düşür.

| № | Müəllimin tələbi | DAG / mexanizm | Təqdim olunan sübut |
|---|---|---|---|
| 1 | Fayl siyahısı və Dynamic Task Mapping | `producer`: `discover_files`, `process_file.expand()` | **4** uğurlu mapped instance |
| 2 | CSV/JSON branch-lərinin seçilməsi | `producer`: `select_branch`, `process_csv`, `process_json` | Hər iki branch seçilib və icra olunub |
| 3 | Join `none_failed_min_one_success`, cleanup `all_done` | `producer`: `join`, `cleanup` | Hər iki task uğurludur; qaydalar kodda görünür |
| 4 | Sales Asset publish, Consumer event ilə başlasın | `producer.publish_sales` → `consumer` | Asset event və avtomatik Consumer run |
| 5 | Catchup qərarı, 3 günlük backfill planı | Konfiqurasiya + bu README | **Plan / izah**, icra edilməyib |
| 6 | Digər DAG-dakı `load_source`-u gözləyən sensor | `consumer.wait_for_ingestion` | Sensor uyğun Producer task-ını tapıb uğurla keçib |
| 7 | Deferrable rejim, Triggerer tələbi | Sensor alternativi + bu README | **Nəzəri izah**, Triggerer testi edilməyib |

![Producer DAG overview](screenshots_second/producer%20dag%20run.png)

---

## 1. Fayl siyahısı və Dynamic Task Mapping

**Sual:** Fayl siyahısını qaytaran task yaradın və həmin siyahını Dynamic Task Mapping ilə emal edin.

**Həll:** `discover_files` `S3Hook.list_keys(bucket_name="advance-airflow")` ilə MinIO obyektlərinin adlarını alır, yalnız `.csv` və `.json` uzantılı key-ləri saxlayır. `process_file.expand(file_name=files)` hər key üçün ayrıca task instance yaradır. Bu mapped task fayl adını və formatını loga yazır; faylın məzmununun oxunması növbəti branch task-larında aparılır.

```python
files = discover_files()
mapped = process_file.expand(file_name=files)
```

**Real nəticə:** `discover_files` logu `curated/sales.csv`, `customers.csv`, `orders.json`, `sales.csv` fayllarını qaytarır; buna görə **4 mapped instance** yaranıb. Əvvəlki sınaqlarda 3 idi; yeni `curated/sales.csv` əlavə olunduğuna görə say 4-ə yüksəlib. Bu nümunədə curated fayl da siyahıya daxil edilir; istehsal layihəsində yalnız raw/source prefix seçmək daha məqsədəuyğundur.

![MinIO discovered files](screenshots_second/discover%20dags%20log.png)

![Four dynamic task instances](screenshots_second/process_%20file.png)

## 2. CSV və JSON branch-ləri

**Sual:** Fayl formatına görə CSV və JSON branch-lərini seçin.

**Həll:** `@task.branch` ilə yazılmış `select_branch(files)` siyahıda hansı fayl formatlarının mövcud olduğunu yoxlayır: `.csv` varsa `process_csv`, `.json` varsa `process_json` task ID-sini qaytarır. Hər iki format olduqda **hər iki branch** işləyir.

```python
@task.branch
def select_branch(files):
    chosen = []
    if any(f.lower().endswith(".csv") for f in files):
        chosen.append("process_csv")
    if any(f.lower().endswith(".json") for f in files):
        chosen.append("process_json")
    return chosen
```

**Real nəticə:** `['process_csv', 'process_json']` seçilib. `process_csv` logu 3 CSV obyektinin hərəsində 3 sətir olduğunu göstərir (`curated/sales.csv`, `customers.csv`, `sales.csv`). `process_json` `orders.json` məlumatlarını oxuyub. Branch-lər mövcud fayllar üzrə seçilir; bu kod hər fayla ayrıca CSV/JSON branch task yaratmır.

![Branch selection log](screenshots_second/select%20branch%20log.png)

![CSV processing log](screenshots_second/process%20csv%20log.png)

![JSON processing log](screenshots_second/process%20json%20log.png)

## 3. JOIN və CLEANUP trigger rule-ları

**Sual:** JOIN üçün `none_failed_min_one_success`, CLEANUP üçün `all_done` istifadə edin.

```python
@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def join():
    print("Selected branches succeeded")

@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup():
    print("Cleanup done")
```

- **`none_failed_min_one_success`**: JOIN-in upstream task-larından heç biri `failed`/`upstream_failed` olmamalıdır, ən azı biri `success` olmalıdır. Digər branch `skipped` olsa da JOIN işləyə bilər.
- **`all_done`**: CLEANUP bütün *birbaşa upstream* task-ları terminal statusa keçəndən sonra işləməyə uyğundur; onların hamısının `success` olması tələb edilmir. `cleanup`-ın öz uğuru isə kodundan asılıdır.

**Real nəticə:** Göndərilən run-da hər iki branch uğurlu olduğuna görə `join` və `cleanup` da `success` olub. **Failure və ya skipped branch ssenarisi bu yeni 2-DAG run şəkillərində ayrıca sübut olunmur.** `publish_sales` yalnız JOIN və CLEANUP uğurlu olduqdan sonra işləyir.

![Join success log](screenshots_second/join%20log.png)

![Cleanup success log](screenshots_second/cleanup%20log.png)

## 4. Producer → Sales Asset → Consumer

**Sual:** Producer DAG `sales` Asset-i publish etsin; Consumer DAG Asset event ilə başlasın.

**Producer:** `publish_sales` MinIO-da `sales.csv` faylını oxuyur, boş olmadığını yoxlayır və `curated/sales.csv` obyektinə yazır. `@task(outlets=[SALES_ASSET])` vasitəsilə uğurlu task icrasında Asset event yaradılır.

**Consumer:** `schedule=[SALES_ASSET]` olduğuna görə bu event yeni Consumer DAG Run yaradır. Consumer, sensor uğurla bitəndən sonra həmin curated CSV-ni oxuyur.

```python
SALES_ASSET = Asset("s3://advance-airflow/curated/sales.csv")

# producer.py
@task(outlets=[SALES_ASSET])
def publish_sales():
    ...  # MinIO-ya curated/sales.csv yazılır

# consumer.py
with DAG(dag_id="consumer", schedule=[SALES_ASSET], catchup=False, ...) as dag:
    ...
```

**Real nəticə:** Producer 3 satış sətrini publish edib. Airflow Assets ekranında `s3://advance-airflow/curated/sales.csv` üçün **1 producing task** və **1 scheduled DAG** görünür. Producer Overview panelində yeni Asset event-in **consumer DAG Run yaratdığı və onun uğurlu olduğu** görünür. MinIO-da `curated/sales.csv` mövcuddur. Ekranda görünən köhnə `curated/sales.txt` daha əvvəlki təcrübədən qalıb və bu iki-DAG axınına aid deyil.

> Yalnız MinIO-ya xaricdən obyekt yükləmək Airflow Asset event-i yaratmır. Bu layihədə event `publish_sales` task-ının uğurla tamamlanması ilə emit olunur.

![Sales publish log](screenshots_second/publish%20sales.png)

![Asset listing](screenshots_second/assets%20view.png)

![MinIO curated file](screenshots_second/minio%20view.png)

## 5. Catchup qərarı və 3 günlük backfill planı

**Sual:** `catchup` qərarını izah edin və 3 günlük backfill planı hazırlayın.

**Qərar:** Hər iki DAG-da `catchup=False` saxlanılır. Producer hazırda `schedule=None` ilə **manual** başlanır; Consumer `schedule=[SALES_ASSET]` ilə **hadisə əsaslı** başlanır. Bu iki konkret schedule üçün keçmiş gündəlik data intervalını Airflow avtomatik yaratmır. `catchup=False` burada avtomatik köhnə periodların kütləvi icrasının nəzərdə tutulmadığını ifadə edir; **özlüyündə 3 günlük backfill yaratmır**.

**Plan: 17, 18 və 19 oktyabr 2026 (UTC) üçün tarixi satış datasını təkrar işləmək.**

| Addım | Tədbir | Gözlənilən yoxlama |
|---|---|---|
| 1 | Mənbədə hər günə aid satış faylı/partisiyası hazırlamaq | Hər tarix üçün ayrı input olmalıdır |
| 2 | Producer-i tarixi emala uyğunlaşdırmaq: seçilən günü `data_interval_start`/parametr vasitəsilə oxumaq və nəticəni tarixlə ayrılmış key-də yazmaq | Gündəlik nəticələr bir-birini əvəz etməsin |
| 3 | Backfill üçün ayrıca deploy/test konfiqurasiyasında Producer-ə gündəlik timetable (`@daily`) təyin etmək; `start_date` tarixi intervalları əhatə etməlidir | Time-based backfill üçün uyğun schedule |
| 4 | 17–19 oktyabr üçün `--dry-run` ilə intervalları yoxlamaq | 3 planlı gündəlik interval |
| 5 | Həqiqi backfill-i `max_active_runs=1` ilə icra etmək | Hər gün üçün Producer nəticəsi və data key-ləri |
| 6 | Producer hər uğurlu emaldan sonra Asset event emit edir; Consumer-in event-lə başlamasını ayrıca müşahidə etmək | Consumer run-ları və event mapping yoxlanılır |

Airflow 3.1 üçün **planlaşdırılan** əmrlər (yalnız Producer gündəlik schedule-a uyğunlaşdırılıbsa):

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id producer --from-date 2026-10-17 --to-date 2026-10-19 --reprocess-behavior none --max-active-runs 1 --dry-run

docker compose exec airflow-scheduler airflow backfill create --dag-id producer --from-date 2026-10-17 --to-date 2026-10-19 --reprocess-behavior none --max-active-runs 1
```

**Vacib məhdudiyyət:** Hal-hazırdakı `producer` DAG-ında `schedule=None`-dur və kod `sales.csv`-ni tarixə görə filtr etmir. Buna görə yuxarıdakı backfill əmrlərini **mövcud kodda dəyişiklik etmədən işlətmək düzgün deyil**. Consumer-i də gündəlik time-based backfill kimi təqdim etmirik: o, Asset event ilə başlayır. **Bu tapşırıq üzrə README-də plan hazırlanıb, 3 günlük backfill faktiki icra edilməyib.**

## 6. ExternalTaskSensor — başqa DAG-dakı `load_source`

**Sual:** Başqa DAG-dakı `load_source` task-ını gözləyən `ExternalTaskSensor` əlavə edin.

**Həll:** `consumer.wait_for_ingestion`, `producer.load_source` task-ının `success` olmasını yoxlayır. Consumer Asset event ilə başladığına görə onun logical date-i Producer run-ının logical date-i ilə avtomatik eyni deyil. Bunun üçün Producer `publish_sales` task-ında Asset event-in `extra` sahəsinə öz logical date-ini yazır; Consumer-də `producer_logical_date` funksiyası bu məlumatı oxuyub `execution_date_fn` vasitəsilə sensora ötürür.

```python
# producer.py — publish_sales daxilində
context = get_current_context()
context["outlet_events"][SALES_ASSET].extra = {
    "producer_logical_date": context["logical_date"].isoformat()
}

# consumer.py
wait_for_ingestion = ExternalTaskSensor(
    task_id="wait_for_ingestion",
    external_dag_id="producer",
    external_task_id="load_source",
    execution_date_fn=producer_logical_date,
    allowed_states=["success"],
    failed_states=["failed"],
    poke_interval=10,
    timeout=300,
    mode="reschedule",
    deferrable=False,
)
```

**Real nəticə:** Son Consumer run **asset-triggered** olub və hər iki task (`wait_for_ingestion`, `mart_build`) `success`-dur. Sensorun logunda `Poking for tasks ['load_source'] in dag producer` və `Success criteria met. Exiting.` görünür. Bu run-da Producer artıq tamamlandığı üçün **sensorun gözləyib reschedule edilməsini göstərən ayrıca log yoxdur**; sadəcə düzgün xarici task vəziyyətini aşkarladığı sübut olunur.

`mart_build` MinIO-dan `curated/sales.csv` faylını oxuyur və **3 satış sətrini** loglayır. Nümunədə `mart_build` real SQL mart cədvəli yaratmır, oxuma/emal mərhələsinin sadə nümayişidir.

![Consumer overview](screenshots_second/consumer%20dag%20run.png)

![ExternalTaskSensor log](screenshots_second/wait%20for%20ingestion%20log.png)

![Mart build reads three rows](screenshots_second/mart%20build%20log.png)

## 7. Deferrable rejim və Triggerer tələbi

**Sual:** Uyğun sensor üçün deferrable rejimini və Triggerer tələbini izah edin.

| Rejim | Gözləmə zamanı worker slotu | Necə işləyir? | Triggerer |
|---|---|---|---|
| `poke` | Tutulur | Worker sensoru təkrar yoxlayır | Lazım deyil |
| `reschedule` | Yoxlamalar arasında boşalır | Scheduler sensoru növbəti yoxlama üçün yenidən planlaşdırır | Lazım deyil |
| `deferrable=True` | Defer-dən sonra boşalır | Async Triggerer hadisəni izləyir; şərt ödənəndə task yenidən worker-ə qayıdır | **Tələb olunur** |

**Dörd mərhələ:** `Worker starts → Defer → Triggerer waits → Resume on worker`.

`ExternalTaskSensor` deferrable rejimi dəstəkləyir. Nəzəri alternativ:

```python
ExternalTaskSensor(
    task_id="wait_for_ingestion",
    external_dag_id="producer",
    external_task_id="load_source",
    execution_date_fn=producer_logical_date,
    poke_interval=10,
    timeout=300,
    deferrable=True,
)
```

- Triggerer ayrıca Airflow prosesi (`airflow triggerer`) kimi aktiv olmalıdır; Docker Compose-a müvafiq servis əlavə etmək olar.
- Sensor/operator və onun provider versiyası `deferrable` rejimini dəstəkləməlidir.
- Triggerer işləməsə deferred task davam etməyə bilər.
- Uzun gözləmə və çoxsaylı sensorlarda deferrable daha resurs-səmərəlidir; qısa gözləmədə fərq kiçik ola bilər.

**Layihədə praktik olaraq `mode="reschedule", deferrable=False` istifadə edilib. Triggerer servisi quraşdırıldığı və ya deferrable run icra edildiyi iddia olunmur.**

## Quruluş və işə salma

```text
advance-airflow/
├── airflow/
│   └── dags/
│       ├── producer.py
│       └── consumer.py
├── screenshots/
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .gitignore
└── README.md
```

Göstərilən iki fayl müəllimin qiymətləndirəcəyi DAG-lardır. Digər köhnə test DAG-ları lokal mühitdən təqdimat zamanı kənara çıxarıla bilər. `.env` və şifrələr GitHub-a yüklənməməlidir. MinIO-da `advance-airflow` bucket-i, başlanğıc `sales.csv`, `customers.csv`, `orders.json` obyektləri və Airflow-da `minio_s3` AWS tipli Connection olmalıdır.

```powershell
docker compose up -d
docker compose exec airflow-scheduler airflow version
docker compose exec airflow-scheduler airflow dags list-import-errors
```

- Airflow UI: `http://localhost:8081`
- MinIO UI: `http://localhost:9003`
- MinIO endpoint (Docker şəbəkəsi): `http://minio:9000`

**Test qaydası:** Hər iki DAG unpaused olsun → yalnız **`producer`** üçün manual Trigger → `publish_sales` success → yeni Asset event → **`consumer`** avtomatik run → `wait_for_ingestion` və `mart_build` success. Əvvəlki uğursuz run yeni uğurlu run-ın nəticəsini dəyişmir.

## Yekun və yoxlama sərhədləri

- **Praktik təsdiqlənənlər:** 4 mapped task instance, CSV/JSON branch-ləri, uğurlu JOIN və CLEANUP, MinIO curated sales faylı, sales Asset event ilə Consumer-in avtomatik işləməsi, ExternalTaskSensor success, mart_build-in 3 sətir oxuması.
- **Plan/nəzəri hissələr:** 3 günlük backfill, `deferrable=True` və Triggerer.
- **Ayrıca yoxlanılmayanlar:** branch failure/skipped ssenarisində cleanup davranışı, 3 tarix üçün real backfill, deferrable sensorun Triggerer ilə faktiki gözləməsi.

### Mənbələr

- [Airflow Backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html)
- [Airflow Deferrable Operators & Triggers](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
- [Airflow Asset Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
