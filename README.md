# 6주차 산출물 — 실시간 CDC 파이프라인

MySQL의 변경 기록을 Debezium으로 수집하고 Kafka를 거쳐 MinIO에 JSON 파일로 저장하는 실습.

## 1. 구성

| 서비스 | 역할 | 포트 |
| --- | --- | --- |
| kafka | 이벤트 중간 저장소 | 9092(외부), 29092(컨테이너 내부) |
| mysql | 원천 DB | 3306 |
| connect | Debezium 소스 및 S3 Sink 실행 | 8083 |
| minio | S3 호환 적재 대상 | 9000(API), 9001(콘솔) |

사용 이미지: `apache/kafka:3.7.0`, `mysql:8.0`, `debezium/connect:2.7.3.Final`, `pgsty/minio`.
S3 Sink 플러그인 버전은 `12.1.15`임.

## 2. 데이터 흐름

```text
MySQL(binlog) → Debezium(mysql-cdc) → Kafka(shop.shop.orders)
             → S3 Sink(minio-sink) → MinIO(cdc-raw)
```

## 3. 토픽 구성

| 토픽 | 용도 |
| --- | --- |
| orders | 파이썬 생산자·소비자 실습 |
| orders-test | flush 생략 시 전송 결과 확인 |
| shop.shop.orders | 주문 CDC 이벤트 |
| schema-changes.shop | Debezium 내부 스키마 이력 |
| connect_configs | Connect 설정 |
| connect_offsets | Connect 소스 오프셋 |
| connect_statuses | Connect 상태 |
| __consumer_offsets | 소비자 그룹 커밋 오프셋 |
| shop | 추가 관찰된 토픽; 메시지 용도는 별도 확인 필요 |

## 4. 제출 파일

- `docker-compose.yml`: 실제 실습 서비스 설정
- `register-mysql.json`: Debezium 소스 커넥터 설정
- `sink-minio.json`: S3 Sink 커넥터 설정

공개 제출본에서는 실제 비밀번호를 제거함. 실행 중인 WSL 실습 파일은 변경하지 않았음.
Compose 실행 전 `.env.example`을 `.env`로 복사하고 실제 값을 입력해야 함.
커넥터 JSON을 각각 `register-mysql.local.json`, `sink-minio.local.json`으로 복사하고, `REPLACE_WITH_...` 값을 실제 자격증명으로 교체한 뒤 등록해야 함. JSON 자리표시자는 자동 치환되지 않음.
Debezium 비밀번호는 MySQL에서 만든 사용자와, MinIO 키는 해당 MinIO 계정과 일치해야 함.

Compose 실행 전 S3 Sink 플러그인을 `plugins/confluentinc-kafka-connect-s3`에 설치.
MySQL의 `shop.orders` 테이블과 Debezium 사용자·권한, MinIO의 `cdc-raw` 버킷은 별도로 생성.

## 5. 확인한 것

2026-11-05 실행 환경을 읽기 전용으로 점검함.

- MySQL에서 `log_bin=ON`, `binlog_format=ROW` 확인(실습 출력).
- `mysql-cdc`, `minio-sink` 모두 커넥터와 task 0이 `RUNNING`.
- Kafka 이벤트를 처음부터 조회하여 `r=2`, `c=14`, `u=6`, `d=1`을 확인. 이 수치는 점검 시점의 이벤트 건수이며 현재 테이블 행 수가 아님. null tombstone은 제외함.
- MinIO `cdc-raw/topics/shop.shop.orders/partition=0/`에 JSON 객체 4개 확인.
- 객체 시작 오프셋: `0000000000`, `0000000005`, `0000000011`, `0000000016`.
- 마지막 객체를 S3 API로 읽어 INSERT 이벤트와 주문 데이터를 확인함. MySQL 변경이 MinIO 파일까지 전달되는 흐름이 확인.
- Sink 설정은 `flush.size=5`, `behavior.on.null.values=ignore`. 마지막 객체는 JSON 레코드 5줄로 확인. 파일 생성 전후를 새로 재현한 실험은 수행하지 않았음.

저장 파일에는 `schema`와 `payload`가 함께 있습니다. Sink의 `value.converter.schemas.enable=false` 설정이 기존 이벤트의 `schema` 필드를 제거하는 것은 아님.

## 6. 남은 개선점

- 기존 `spark-minio`와 9000·9001 포트가 충돌했으므로 실행 전 포트 사용 여부를 확인한다.
- MySQL 시작 직후 접속 실패를 방지하도록 healthcheck와 준비 완료 대기를 추가한다.
- 현재 Compose에 명시적인 데이터 볼륨 설정이 없으므로 서비스별 데이터 영속화를 보완한다.
- 기본 비밀번호를 분리하고 외부 공개 포트를 필요한 범위로 제한한다.
- 반복 INSERT로 테스트 데이터가 중복 추가될 수 있으므로 초기화와 검증 절차를 구분한다.
- 소량 이벤트가 오래 대기하는 상황을 줄이도록 파일 회전 정책을 검토한다.
- 커넥터 상태, 소비 지연, MinIO 객체 생성 여부를 함께 점검한다.

## 7. 점검 명령

WSL에서 실행합니다. 마지막 Compose 명령은 이 파일들이 있는 폴더에서 실행합니다.

```bash
curl -s http://localhost:8083/connectors/mysql-cdc/status | python3 -m json.tool
curl -s http://localhost:8083/connectors/minio-sink/status | python3 -m json.tool
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connect-minio-sink
docker compose logs --tail 30 connect
```
