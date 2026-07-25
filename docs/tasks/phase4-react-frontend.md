# Task Phase 4: Frontend Web Application 개발 (Vite + React)

* **목표**: 독립된 React 웹 애플리케이션을 구축하여 실시간 추론 타임라인 및 마크다운 타자기 스트리밍 UI를 구현합니다.
* **관련 문서**: [기술 명세서](../3.spec.md) | [마스터 체크리스트](README.md)

---

## 세부 작업 항목 (Sub-tasks)

- [x] **Task 4.1: `Vite + React + TypeScript` 프로젝트 셋업**
  * `frontend/` 디렉터리에 Vite React App 생성 완료
  * `lucide-react`, `react-markdown`, `remark-gfm` 패키지 설치 완료

- [x] **Task 4.2: Vanilla CSS 기반 디자인 시스템 토큰 구축**
  * 모던 다크 테마, Glassmorphism, CSS 변수 및 Keyframe 애니메이션 정의 완료 (`index.css`, `App.css`)

- [x] **Task 4.3: `useAgentStream` 커스텀 훅 개발**
  * SSE 연결 수립, `sessionId` 수신, `STATUS/CHUNK/DONE/ERROR` 이벤트 핸들링 및 질문 POST 전송 훅 완성 (`useAgentStream.ts`)

- [x] **Task 4.4: 에이전트 추론 타임라인 컴포넌트 (`ChatTimeline`) 개발**
  * `type: STATUS` 이벤트를 수신하여 에이전트 추론 단계(Search, Scrape) 시각화 완료 (`ChatTimeline.tsx`)

- [x] **Task 4.5: 리포트 타자기 스트리밍 컴포넌트 (`ReportViewer`) 개발**
  * `type: CHUNK` 이벤트를 수신하여 마크다운 텍스트 실시간 타자기 효과 및 자동 하단 스크롤 렌더링 완성 (`ReportViewer.tsx`)

- [x] **Task 4.6: 에러 토스트 및 자동 재연결 UI 구현**
  * `type: ERROR` 수신 시 알림 토스트 렌더링 및 SSE 재연결 버튼 UI 구현 완료 (`App.tsx`)
