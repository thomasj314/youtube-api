# YouTube Data Pipeline (AWS Lambda + S3 + Glue + Athena + dbt)

이 저장소는 **ingestion은 AWS**, **모델링은 Athena + dbt** 구조를 기준으로 운영되는 파이프라인입니다.

## 1) 목표
- 신뢰 가능한(재시도/재실행 안전) 데이터 엔지니어링 파이프라인
- **Idempotent** 처리 보장 (같은 입력에 대해 여러 번 실행해도 결과 동일)
- Athena + dbt 기반의 분석/서빙 계층 안정화
- 이후 Semantic Layer / Text2SQL 확장 가능하도록 메타데이터/모델 표준화

---

## 2) 현재 아키텍처

1. **Ingestion Lambda** (`lambda_function/lambda_function.py`)
   - YouTube API 호출
   - Raw JSON을 S3 Bronze에 저장
   - S3 키를 파티션 구조(`region=`, `date=`, `hour=` 등)로 관리

2. **Transform Lambda** (`lambda_function/json_to_parquet_lambda.py`)
   - Bronze JSON 스캔
   - 데이터셋별 파싱/정제
   - Silver Parquet로 변환 후 Glue Catalog 테이블 갱신 (`awswrangler.s3.to_parquet`)
   - `mode="overwrite_partitions"` 사용으로 파티션 단위 멱등성 강화

3. **Orchestration** (`step_functions/youtube_pipeline.asl.json`)
   - Ingestion Lambda → Transform Lambda
   - 실패 시 SNS 알림

4. **Serving/Modeling** (`dbt/`)
   - Athena를 쿼리 엔진으로 사용
   - dbt 모델/테스트를 통해 Gold 마트 구성

---

## 3) 신뢰성과 멱등성(idempotency) 설계 원칙

### A. Ingestion (Bronze)
- 동일 파티션 키에 대해 파일 **덮어쓰기 가능**한 키 설계 유지
- 재시도 시 중복 파일이 아닌 동일 키 overwrite를 우선
- API quota 초과 시 즉시 fail-fast (불필요한 호출 차단)

### B. Transform (Silver)
- 소스 파일을 파티션별로 그룹핑 후 처리
- 파티션 결과를 `overwrite_partitions`로 기록
- 문자열 정제/타입 캐스팅을 일관되게 수행
- 비정상 파일은 전체 중단 대신 결과에 오류를 수집하고 알림

### C. Athena + dbt (Gold)
- 모델은 가능하면 `incremental` + 고유키 전략 적용
- late-arriving 데이터 대응을 위해 최근 N일 재처리 윈도우 권장
- 필수 테스트:
  - `not_null`
  - `unique`
  - `relationships`
  - source freshness

---

## 4) 운영 체크리스트

### 배포/실행 전
- Lambda 환경변수 설정 확인
  - `YOUTUBE_API_KEY`, `S3_BUCKET_BRONZE`
  - `S3_BUCKET_SILVER`, `GLUE_DB_SILVER`
  - 선택: `SNS_ALERT_TOPIC_ARN`, `ENABLE_GOLD`, `GLUE_DB_GOLD`
- Step Functions ARN 템플릿 변수 치환 확인
- Athena workgroup/결과 버킷/dbt profile 점검

### 일일 점검
- Step Functions 실패율
- Lambda 에러/타임아웃/재시도 건수
- Glue Catalog 파티션 반영 여부
- dbt test 실패 추이

---

## 5) 향후 확장 (Semantic Layer / Text2SQL)

1. dbt 모델에 metric 후보(핵심 KPI) 표준화
2. 컬럼 설명/용어 사전(documentation) 강화
3. Semantic Layer 연결 전, dbt test 100% 통과 기준선 확보
4. Text2SQL 도입 시 안전장치:
   - 허용 스키마/테이블 화이트리스트
   - DDL/DML 차단
   - LIMIT 강제
   - 쿼리 비용/시간 제한

---

## 6) 로컬 개발 메모

```bash
# (dbt 프로젝트 디렉토리에서)
dbt deps
dbt debug
dbt run
dbt test
```

> 참고: 실제 실행을 위해서는 `profiles.yml`에 Athena 연결 정보가 필요합니다.


## Trend Mart 표준 및 Text2SQL 안전장치

- Gold 마트 `youtube_trending`은 `dbt/models/marts/youtube_trending.sql`에서 incremental + insert_overwrite 전략으로 유지합니다.
- 중복 방지는 `trend_record_id` 유니크 키와 `(collection_date, region_code, video_id)` 조합 유니크 테스트로 검증합니다.
- KPI 후보는 다음 3가지를 기본 표준으로 둡니다.
  - `kpi_trending_video_count`
  - `kpi_total_views`
  - `kpi_avg_like_rate`
- Semantic Layer 연계 전 기준선은 `dbt test` 100% 통과입니다.
- Text2SQL 운영 시 필수 가드:
  - 화이트리스트 스키마/테이블(`yt_pipeline_gold_dec.youtube_trending` 등)만 허용
  - DDL/DML 금지 (`CREATE`, `DROP`, `ALTER`, `INSERT`, `UPDATE`, `DELETE` 차단)
  - `LIMIT` 강제
  - Athena workgroup의 쿼리 타임아웃/스캔 바이트 제한 활성화

- 참고: 현재 Athena + dbt 구성에서는 Delta Lake를 dbt materialization 대상으로 직접 쓰기 어렵습니다. Gold 마트는 Iceberg(권장) 또는 Hive 테이블로 운영하세요.
