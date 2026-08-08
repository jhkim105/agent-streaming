# 📌 Tech Note 0001: SSE 디버깅, 프록시 동작 원리 및 SPA 배포 라우팅

* **작성일**: 2026-08-02
* **관련 분야**: Frontend, Backend, Network, DevOps

---

## 1. Chrome DevTools에서 SSE(Server-Sent Events) 스트리밍 확인하기

### ❓ 발생했던 의문점
개발자 도구(DevTools) Network 탭에서 질문 등록 요청(`message`)은 보이는데, 실시간 토큰 및 이벤트를 받아오는 `stream` 요청이 보이지 않거나 캡처되지 않음.

### 💡 원인 및 해결 방법
1. **네트워크 필터 범주**:
   - `EventSource`(SSE) 커넥션은 `Fetch/XHR` 카테고리가 아닌 **`All`** 또는 **`Other`** 카테고리에 분류됩니다.
   - DevTools 상단 필터에서 **`All` (전체)**을 선택해야 목록에 표시됩니다.
2. **커넥션 캡처 시점**:
   - SSE 수립 요청(`/api/chat/stream`)은 페이지가 최초 연결되는 시점에 발생합니다.
   - DevTools를 켜기 전에 요청이 완료되었으면 목록에 나타나지 않으므로, **DevTools가 열린 상태에서 페이지 새로고침 (`Cmd + R` 또는 `F5`)**을 수행하면 맨 위에 `stream` 요청이 캡처됩니다.
3. **실시간 이벤트 감시**:
   - `stream` 요청 클릭 $\rightarrow$ 오른쪽 상세 패널의 **`EventStream`** 탭을 선택하면 `INIT`, `STATUS`, `CHUNK`, `A2UI_RENDER`, `DONE` 이벤트와 수신 데이터 조각을 실시간 표 형태로 확인할 수 있습니다.

---

## 2. 개발 환경 프록시(Vite Proxy)의 동작 원리

### ❓ 발생했던 의문점
클라이언트 브라우저에서 SSE 연결 주소가 코틀린 백엔드(`http://localhost:8080`)가 아니라 프론트엔드 주소(`http://localhost:5173/api/chat/stream`)로 표시되는 이유.

### 💡 원인 및 동작 원리
1. **CORS (Cross-Origin Resource Sharing) 방지**:
   - 브라우저 보안 규칙상 5173 포트(FE)에서 8080 포트(BE)로 다이렉트 요청 시 발생할 수 있는 CORS 문제를 방지합니다.
2. **Vite 개발 서버의 Reverse Proxy**:
   - `vite.config.ts` 설정에 의해 `/api`로 시작하는 모든 요청을 Vite 개발 서버가 받아서 백엔드 서버(`http://localhost:8080`)로 실시간 포워딩(Pass-through)합니다.

```text
[브라우저 (5173)] ──(1) GET /api/chat/stream──► [Vite Dev Server (5173)] ──(2) Proxy Forwarding──► [Kotlin Stream Server (8080)]
```

---

## 3. SPA(Single Page Application) 정적 배포 및 필수 라우팅

### ❓ 발생했던 의문점
운영 환경으로 배포할 때 Node.js 프론트엔드 서버를 띄워야 하는지, Nginx나 AWS S3/CloudFront 같은 정적 호스팅 배포 시 어떤 라우팅 설정이 필요한지.

### 💡 결론 및 배포 모범 사례
React(Vite) 앱은 `npm run build` 시 단순 정적 파일 번들(`dist/` 폴더)로 변환되므로 **별도의 Node.js FE 서버 구동 없이 Nginx 또는 AWS S3 + CloudFront 정적 배포가 정석**입니다.

정적 배포 시 **필수 라우팅 2가지**:

#### 1) 백엔드 API 및 SSE 프록시 라우팅 (`/api/*`)
- 정적 웹서버/CDN에 `/api/*`로 시작하는 요청은 코틀린 백엔드(ALB/Nginx) 주소로 프록시 전달하도록 라우팅 설정.

#### 2) SPA 404 Fallback 라우팅
- React SPA는 `index.html` 단일 파일 구조이므로, 사용자가 특정 경로에서 새로고침 시 404 Not Found가 발생하지 않도록 실제 파일이 없을 경우 `index.html`로 되돌려주는 라우팅 추가.
  - Nginx: `try_files $uri $uri/ /index.html;`
  - CloudFront: Custom Error Response (404 $\rightarrow$ 200 `/index.html`)

#### ⚠️ 운영 배포 시 SSE 무한 스트리밍 필수 설정
- Nginx 사용 시 SSE 토큰 조각이 즉시 바로바로 클라이언트로 흘러가도록 **`proxy_buffering off;`** 설정 필수.
- 장시간 리서치를 위해 **`proxy_read_timeout 3600s;`** 연장 설정.
