# 🚦 نظام مراقبة حركة المرور بالرادار الذكي (Smart Radar Traffic System)

**مشروع تخرج (Capstone Project) يوضح بناء خط أنابيب بيانات (Data Pipeline) متكامل وشامل (End-to-End) على سحابة Azure، تم تنفيذه كجزء من مبادرة eyouth X DEPI.**

هذا المشروع هو تطبيق عملي لبناء بنية "Lambda Architecture" حديثة، ويغطي دورة حياة بيانات المرور بالكامل: بدءًا من المحاكاة اللحظية للبيانات (M1)، تجميعها وحفظها (Ingestion)، معالجتها بشكل دفعي (Batch Processing - M2)، معالجتها لحظيًا (Streaming - M3)، وحتى عرضها في لوحة معلومات تفاعلية (M4).

![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Synapse](https://img.shields.io/badge/Azure%20Synapse-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

---

## 🏛️ 1. البنية التقنية للمشروع (Final Architecture)

لقد قمنا ببناء بنية "Lakehouse" حديثة وفعالة من حيث التكلفة، بالاعتماد الكامل على خدمات Azure المُدارة. هذه البنية تجمع بين المسار "البارد" (Batch) والمسار "الساخن" (Streaming) في تصميم Lambda Architecture.

![البنية التقنية الكاملة للمشروع](architecture_diagram.png)

---

### مسار تدفق البيانات (Data Flow) بالتفصيل

#### المرحلة الأولى: M1 - إنتاج وتجميع البيانات

1.  **(الإنتاج) Producer:** خدمة `Azure Container Instance (ACI)` تقوم بتشغيل حاوية (Container) تحتوي على اسكربت Python (`streaming_radar_producer.py`). يعمل هذا الاسكربت 24/7 كمحاكي، حيث يولد بيانات رادار وهمية (JSON) ويرسلها إلى السحابة.
2.  **(التجميع) Ingestion:** يتم إرسال جميع البيانات (JSON) إلى `Azure Event Hub` (`radar-hup`)، الذي يعمل "كنقطة تجميع" (Ingestion Hub) أولية قادرة على استيعاب ملايين الرسائل.
3.  **(الجسر) Bridge:** وظيفة `Azure Stream Analytics (ASA)` (الجسر) تقرأ البيانات من Event Hub وتحفظها كملفات `JSON` خام في `Azure Data Lake Storage (ADLS Gen2)` (`radarcont`). (تم بناء هذا الجسر كحل بديل لأن طبقة "Basic" من Event Hub لا تدعم "Capture").

#### المرحلة الثانية: M2 - المعالجة الدفعية (Batch ELT - Lakehouse)

1.  **(المحرك) Engine:** `Azure Synapse Spark Pool` (`SparkPoolSmall`) (بحجم 3 عقد وإيقاف تلقائي بعد 15 دقيقة لتوفير التكلفة).
2.  **(المنطق) Logic:** `PySpark Notebook` (`daily_elt_job_LAKEHOUSE.py`) يعمل على الـ Spark Pool.
3.  **(التشغيل) Run:** يتم تشغيل الـ Notebook (يدويًا أو تلقائيًا عبر Synapse Pipeline).
4.  **(القراءة) Extract:** الكود يقرأ جميع ملفات `JSON` الخام من `abfss://radarcont@radardatalake1.dfs.core.windows.net/*.json`. (تمت معالجة `+20,000` سجل خام بنجاح).
5.  **(المعالجة) Transform:** يقوم الكود بتنظيف البيانات، "تفجير" (explode) المخالفات، وتجميع (aggregate) الرحلات لإنشاء 4 جداول نظيفة.
6.  **(الكتابة) Load:** يتم حفظ الجداول النظيفة كملفات `Parquet` (تنسيق عمودي عالي الكفاءة) في مجلد `clean-tables` داخل الـ Data Lake.

#### المرحلة الثالثة: M3 - المعالجة اللحظية (Real-time)

1.  **(المنطق) Logic:** وظيفة `Azure Stream Analytics` ثانية (`realtime-violations-job`) تقرأ من `radar-hup` **لحظيًا**.
2.  **(الفلترة) Filter:** يقوم الاستعلام بفلترة البيانات (`WHERE is_violation = true`) لاختيار "المخالفات فقط" في الوقت الفعلي.
3.  **(المخرج) Output:** يتم إرسال هذه المخالفات مباشرة إلى "Power BI Streaming Dataset" لإنشاء لوحة معلومات حية.

#### المرحلة الرابعة: M4 - العرض والتحليل

1.  **(العرض التاريخي) Batch View:** `Synapse Serverless SQL Pool` (المجاني تقريبًا) يوفر واجهة SQL.
2.  قمنا بإنشاء قاعدة بيانات (`RadarLakehouseDB`) و "عروض" (Views) (مثل `vw_fact_journeys`) تقرأ مباشرة من ملفات `Parquet` النظيفة في `clean-tables`.
3.  **(لوحة المعلومات) Dashboard:** يتصل `Power BI` بمصدرين:
    * **لبيانات M2 (التاريخية):** يتصل بـ `Serverless SQL Pool` ويستعلم من الـ Views (مثل `vw_fact_journeys`).
    * **لبيانات M3 (اللحظية):** يتصل بـ `ASA Streaming Dataset` (من M3).

---

## 🛠️ 2. التقنيات المستخدمة بالتفصيل

| المرحلة | الغرض | التقنية المستخدمة |
| :--- | :--- | :--- |
| **M1: الإنتاج** | محاكاة البيانات | `Python` (Faker) |
| | تغليف الخدمة | `Docker` |
| | تخزين الـ Image | `Azure Container Registry (ACR)` |
| | تشغيل الخدمة 24/7 | `Azure Container Instances (ACI)` |
| **التجميع** | نقطة تجميع الرسائل | `Azure Event Hub` (Basic Tier) |
| | جسر حفظ البيانات | `Azure Stream Analytics (ASA)` (Job 1 - Bridge) |
| | تخزين البيانات (Lake) | `Azure Data Lake Storage (ADLS Gen2)` |
| **M2: المعالجة** | مساحة العمل | `Azure Synapse Analytics` |
| | محرك المعالجة | `Apache Spark Pool` (`SparkPoolSmall`) |
| | منطق التحويل (ELT) | `PySpark` (في Synapse Notebook) |
| | عرض البيانات (Lakehouse) | `Synapse Serverless SQL Pool` (Built-in) |
| **M3: اللحظية** | اكتشاف المخالفات | `Azure Stream Analytics (ASA)` (Job 2 - Real-time) |
| **M4: العرض** | لوحة المعلومات | `Power BI` |
| **الجدولة** | تشغيل M2 تلقائيًا | `Synapse Pipelines (Orchestrator)` |

---

## ✨ 3. إنجازات وتحديات (Features & Problem-Solving)

هذا القسم يبرز المهارات الهندسية المستخدمة لحل المشاكل الحقيقية التي واجهت المشروع.

### 🌟 بنية Lakehouse موفرة للتكلفة (الحل الذكي)
* **المشكلة:** الهدف الأصلي كان استخدام `Dedicated SQL Pool` (مستودع بيانات DWH). لكن هذا الخيار **مكلف جدًا** (حوالي 1.84$/ساعة)، وهو ما يستنزف رصيد حساب الطلاب.
* **الحل:** قمنا بتغيير البنية إلى "Lakehouse" فعال من حيث التكلفة.
    1.  كود PySpark (M2) يحفظ النتائج النظيفة كملفات `Parquet` (بدلاً من الكتابة في DWH).
    2.  نستخدم `Serverless SQL Pool` (الذي يُحاسب بالاستعلام فقط بتكلفة ضئيلة) لإنشاء `Views` فوق هذه الملفات.
* **النتيجة:** حصلنا على نفس قوة الـ DWH (جداول SQL نظيفة وقابلة للاستعلام) بتكلفة شبه صفرية، مما يظهر قدرة على تصميم حلول فعالة اقتصاديًا.

### 🌉 حل مشكلة Event Hub "Basic Tier"
* **المشكلة:** طبقة "Basic" لـ Event Hub (التي نستخدمها لتوفير التكلفة) لا تدعم ميزة "Capture" لحفظ البيانات تلقائيًا في Data Lake.
* **الحل:** قمنا ببناء "جسر بيانات" (Data Bridge) مخصص باستخدام وظيفة `Stream Analytics` (بتكلفة `1/3 SU`)، تقوم بقراءة `SELECT *` من Event Hub وحفظها كملفات `JSON` في الـ Data Lake. هذا يظهر مرونة وقدرة على إيجاد حلول بديلة.

### 🐛 تصحيح أخطاء النشر (End-to-End Debugging)
* **المشكلة:** واجهنا سلسلة من الأخطاء المعقدة عند محاولة نشر خدمة `ACI`.
* **الحل:** تم تصحيح الأخطاء بشكل منهجي:
    1.  **`TasksOperationsNotAllowed`**: تجاوزنا قيود الاشتراك (`Azure for Students`) بالتحول من `az acr build` (بناء سحابي) إلى `docker build` + `docker push` (بناء محلي).
    2.  **`No such file or directory`**: اكتشفنا عدم تطابق اسم الملف في `Dockerfile` وقمنا بإصلاحه.
    3.  **`UnboundLocalError`**: قمنا بتشغيل الـ Image محليًا (`docker run ...`) لاكتشاف خطأ بايثون داخلي وإصلاحه قبل إعادة الرفع.

### 🚫 حل تعارض القراءة/الكتابة في Spark
* **المشكلة:** بعد تشغيل كود PySpark بنجاح، وجدنا أن تشغيله مرة ثانية يتسبب في اختفاء البيانات (الملفات أصبحت فارغة).
* **الحل:** اكتشفنا أن `spark.read.format("json")` كان يقرأ من المسار الرئيسي (`/radarcont/`)، والذي يحتوي على **كل من** الـ `JSON` الخام ومجلد `clean-tables` (الذي يحتوي على `Parquet`). عندما حاول قراءة Parquet كـ JSON، فشل وقرأ 0 سجل. تم إصلاح هذا عن طريق جعل مسار القراءة أكثر تحديدًا: `.../radarcont/*.json`.

---
## 📁 4. هيكل الملفات (Repository Structure)

تم تنظيم المشروع في مجلدات منفصلة تمثل كل مرحلة من مراحل خط أنابيب البيانات، لفصل المهام وضمان سهولة الصيانة.

* **`/` (المجلد الرئيسي)**
    * `README.md`: (هذا الملف) الواجهة الرئيسية للمشروع التي تشرح كل شيء.
    * `.gitignore`: ملف لتجاهل الملفات غير الضرورية (مثل `__pycache__` و `venv`).
    * `architecture_diagram.png`: رسم بياني يوضح البنية التقنية الكاملة للمشروع.

* **`/1_Producer_Service`**
    * `streaming_radar_producer.py`: اسكربت البايثون "النظيف" (M1) الذي يحاكي البيانات ويرسلها إلى Event Hub.
    * `Dockerfile`: ملف "الوصفة" لبناء حاوية (Container) قابلة للنشر لخدمة ACI.
    * `requirements.txt`: يحتوي على المكتبات (`faker`, `azure-eventhub`) اللازمة للاسكربت.

* **`/2_Ingestion_Bridge`**
    * `asa_bridge_query.sql`: يحتوي على كود SQL (`SELECT * INTO...`) الخاص بوظيفة Stream Analytics (الجسر) التي تحفظ البيانات الخام (JSON) في الـ Data Lake.

* **`/3_Batch_Processing_M2`**
    * `lakehouse_elt_job.py`: كود PySpark (M2) الذي يقرأ الـ JSON الخام ويحفظ النتائج النظيفة كملفات Parquet (بنية Lakehouse).
    * `(Optional) dwh_elt_job.py`: (ملف اختياري) كود PySpark البديل الذي يحفظ البيانات في "Dedicated SQL Pool" (الـ DWH المدفوع).

* **`/4_Database_Schema`**
    * **`/serverless_views`** (الخيار المجاني):
        * `1_create_database.sql`: كود `CREATE DATABASE RadarLakehouseDB;`.
        * `2_create_views.sql`: يحتوي على جميع أوامر `CREATE VIEW` (مثل `vw_fact_journeys`) التي تقرأ من ملفات Parquet.
    * **`/dedicated_dwh`** (الخيار المدفوع):
        * `1_create_tables_DDL.sql`: كود `CREATE TABLE` (المعدل بـ `IDENTITY`) لإنشاء الجداول السبعة في الـ DWH.
        * `2_seed_data_DML.sql`: كود `INSERT` لملء الجداول الثابتة (`dim_routes`, `dim_radars`).

* **`/5_Streaming_Processing_M3`**
    * `asa_realtime_violations.sql`: كود SQL الخاص بوظيفة Stream Analytics (M3) التي تكتشف المخالفات (`WHERE is_violation = true`) وترسلها إلى Power BI.

* **`/6_Dashboard_M4`**
    * `dashboard_screenshot.png`: لقطة شاشة للوحة المعلومات (Dashboard) النهائية.








---

## 📊 5. النتائج النهائية (M2 Output)

بعد نجاح تشغيل `daily_elt_job_LAKEHOUSE.py`، أصبحنا قادرين على الاستعلام عن البيانات النظيفة (Parquet) مباشرة باستخدام SQL (عبر Serverless Pool).

#### الاستعلام عن ملخص الرحلات (Fact Journeys)
```sql
USE RadarLakehouseDB;

SELECT 
    plate,
    total_fines,
    total_violations,
    total_distance
FROM 
    vw_fact_journeys
WHERE
    total_fines > 500
ORDER BY 
    total_fines DESC;
