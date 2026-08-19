# Task Phase 1: 로컬 개발 환경 및 인프라 구축

* **목표**: Kafka 및 Redis 미들웨어를 안정적으로 기동하고 개발 디버깅 환경을 조성합니다.
* **관련 문서**: [기술 명세서](../spec.md) | [마스터 체크리스트](README.md)

---

## 세부 작업 항목 (Sub-tasks)

- [x] **Task 1.1: `docker-compose.yml` 작성**
  * KRaft 모드(Zookeeper 불필요) Kafka 서비스 정의 (포트 `9092`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"`)
  * Redis 서비스 정의 (포트 `6379`, 메모리 512MB 제약)
  * Kafka UI 서비스 정의 (포트 `8989:8080`)

- [x] **Task 1.2: Kafka UI 모니터링 도구 추가**
  * `provectuslabs/kafka-ui` 이미지 추가 (포트 `8989:8080`)
  * Kafka 브로커 연동 설정 및 가시화 완료

- [x] **Task 1.3: Kafka 토픽 생성 스크립트 작성**
  * `agent-requests` 토픽 생성 (파티션 3)
  * `agent-responses` 토픽 생성 (파티션 3)

- [x] **Task 1.4: 로컬 인프라 구동 및 헬스체크 검증**
  * 기존 구동 중인 로컬 Kafka(9092) 및 Redis(6379) 커넥션 핑 확인 완료
  * `agent-requests` 및 `agent-responses` 토픽 정상 생성 검증 완료

