# SoHoLINK Development Guide

This guide covers development workflows, dependency management, and best practices for contributing to SoHoLINK.

---

## Development Setup

### Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| **Go** | 1.25.7+ | Primary language (upgraded 2026-03-03 — CVE GO-2026-4337) |
| **GCC / MinGW** | any | CGO for Fyne GUI (see GUI section below) |
| **Git** | any | Version control |
| **Make** | any | Build automation |
| **goreleaser** | v2+ | Cross-platform release builds |
| **golangci-lint** | latest | Static analysis |
| **opa** | v0.68+ | OPA policy tests (`opa test configs/policies/ -v`) |

### Initial Setup

```bash
# Clone
git clone https://github.com/NetworkTheoryAppliedResearchInstitute/soholink.git
cd soholink

# Download dependencies
go mod download

# Build headless CLI (no CGO, no GCC required)
make build-cli

# Run short tests
make test-short

# Full test suite with race detector
make test
```

### GUI Build (Fyne requires CGO)

Fyne uses OpenGL bindings that require a C compiler:

**Linux:**
```bash
sudo apt-get install gcc libgl1-mesa-dev xorg-dev
make build-gui
```

**macOS:**
```bash
xcode-select --install
make build-gui
```

**Windows (MinGW):**
```powershell
winget install msys2
# Open MSYS2 terminal:
pacman -S mingw-w64-x86_64-gcc
# Add C:\msys64\mingw64\bin to your PATH, then:
make build-gui
# Or for a zero-DLL statically linked binary:
make build-static-windows
```

---

## Project Structure

```
soholink/
├── cmd/
│   └── soholink/           # Unified entry point (//go:build gui → GUI; else → CLI)
│
├── internal/
│   ├── accounting/         # Event recording, JSONL audit logs
│   ├── app/                # Application core wiring
│   ├── auth/               # Ed25519 DID:key credentials
│   ├── blockchain/         # Local tamper-evident Merkle chain
│   ├── cdn/                # Geographic CDN routing + health probing
│   ├── central/            # 1% platform fee calculation
│   ├── cli/                # CLI commands (cobra)
│   ├── compute/            # Firecracker, KVM, Hyper-V, migration, sandbox, cgroups
│   ├── config/             # Configuration management
│   ├── governance/         # Governance proposal + voting
│   ├── gui/
│   │   └── dashboard/      # Fyne GUI: 8-tab dashboard + 8-step wizard (~1,650 lines)
│   ├── httpapi/            # REST API server
│   ├── lbtas/              # Trust + reputation scoring
│   ├── merkle/             # SHA3-256 Merkle batch batcher
│   ├── ml/                 # ML scheduling primitives (no external ML deps — pure Go)
│   │   ├── features.go     #   Dimension constants (NodeFeatureDim=10, ContextDim=20)
│   │   ├── bandit.go       #   LinUCBBandit: disjoint linear UCB; Gauss-Jordan inverse
│   │   └── telemetry.go    #   TelemetryRecorder: JSONL event log; EventBuilder; RewardFor()
│   ├── notification/       # APNs push notifications (iOS)
│   ├── orchestration/      # FedScheduler, auto-scaler, node discovery, K8s edge adapter
│   │   ├── mobile.go       #   NodeClass taxonomy, MobileNodeInfo, MobileTaskDescriptor
│   │   ├── mlfeatures.go   #   Feature extraction (NodeFeatures, BuildContext, SystemState)
│   │   └── scheduler.go    #   ScheduleMobile, SetMLBandit, RecordMobileOutcome
│   ├── payment/            # Stripe, Lightning, metering, ledger, settler, HTLC
│   ├── policy/             # OPA Rego policy engine
│   ├── rental/             # Auto-accept engine
│   ├── sla/                # SLA contract monitoring + credit computation
│   ├── storage/            # IPFS HTTP client (Kubo) + local content-addressed pool
│   ├── store/              # SQLite via modernc.org/sqlite — all persistent state
│   ├── update/             # Auto-update system (planned v1.0)
│   ├── verifier/           # Offline Ed25519 credential verification
│   ├── wasm/               # Wasm task executor (planned v0.3)
│   └── wizard/             # Hardware detection, cost calc, pricing, policy config
│
├── mobile/                 # Mobile applications (planned — see docs/MOBILE_INTEGRATION.md)
│   ├── android-tv/         # Android TV / Fire TV app (Kotlin) — v0.2
│   ├── android/            # Android smartphone app (Kotlin) — v0.3
│   └── ios/                # iOS app (Swift/SwiftUI) — v0.4
│
├── configs/
│   └── policies/
│       ├── resource_sharing.rego      # OPA resource sharing governance
│       ├── resource_sharing_test.rego # OPA policy tests (57 test cases)
│       ├── default.rego               # Default policy rules
│       └── lbtas_gates.rego           # LBTAS trust scoring gate policies
│
├── ui/
│   └── globe-interface/
│       └── ntarios-globe.html     # Three.js 3D globe + WebSocket bridge
│
├── assets/
│   └── logo.svg            # Brand icon source (convert to .png for FyneApp.toml)
│
├── installer/
│   └── windows/            # NSIS .nsi script, logo.ico, banner bitmaps
│
├── scripts/
│   ├── build-all.ps1       # One-command all-platform build (Windows dev)
│   ├── build-installer-windows.ps1
│   └── test.sh / test.bat
│
├── docs/
│   ├── ARCHITECTURE.md     # System design + mobile node architecture
│   ├── INSTALL.md          # Installation guide
│   ├── MOBILE_INTEGRATION.md  # Mobile implementation plan (4 phases)
│   ├── OPERATIONS.md       # Day-to-day node management
│   ├── RESOURCE_SHARING.md # Resource sharing policy reference
│   ├── TESTING.md          # Test guide
│   └── research/
│       ├── README.md
│       ├── MOBILE_PARTICIPATION.md   # Research: mobile devices as nodes
│       └── SOHOLINK_CAPABILITIES.md  # Current capability assessment
│
├── .github/
│   ├── dependabot.yml      # Weekly Go + Actions version PRs
│   └── workflows/
│       ├── build.yml       # GoReleaser cross-platform builds + releases
│       ├── test.yml        # Tests, lint, security scan across 3 OS
│       └── update-deps.yml # Monthly bulk dependency upgrade PR
│
├── FyneApp.toml            # Fyne packaging metadata
├── .goreleaser.yml         # Cross-platform release automation
├── Makefile                # Build, test, package, dist targets
├── CHANGELOG.md            # Version history (Keep a Changelog)
├── ROADMAP.md              # Milestone-based forward plan
├── PLAN.md                 # Gap analysis (legacy task tracker)
├── go.mod / go.sum         # Go module definition
└── vendor/                 # Vendored dependencies (offline builds)
```

---

## Build Modes

```bash
# Headless CLI (no CGO, cross-compiles everywhere)
make build-cli

# GUI (CGO + GCC required)
make build-gui

# Statically linked Windows .exe (no MinGW DLLs on user machine)
make build-static-windows

# Fyne-bundled packages (icon + manifest embedded)
make fyne-package-windows
make fyne-package-linux
make fyne-package-macos

# All platforms via GoReleaser (snapshot — no git tag needed)
make dist

# Real release (requires git tag v*)
make dist-release

# Windows developer: single script that installs prereqs + builds everything
.\scripts\build-all.ps1
```

---

## Dependency Management

All dependencies are vendored in `vendor/` for reproducible offline builds.

### Working with Dependencies

```bash
# Add a dependency (then re-vendor)
go get github.com/example/package@latest
go mod tidy
go mod vendor

# Update a specific dependency
go get -u github.com/example/package
go mod tidy
go mod vendor

# Update all (Dependabot / monthly workflow does this automatically)
go get -u ./...
go mod tidy
go mod vendor
```

### Automated Updates

Dependency updates are fully automated — you generally don't need to run these manually:

- **Dependabot** opens individual PRs every Monday for each outdated package (grouped by ecosystem)
- **`update-deps.yml`** workflow runs the first Monday of each month: `go get -u ./...` → `go mod tidy` → `go mod vendor` → full test suite → PR if tests pass

To trigger a manual bulk update:
1. Go to **Actions** → **Update Dependencies** → **Run workflow**
2. Optionally check **dry run** to preview changes without opening a PR

---

## Testing

```bash
# All tests with race detector + coverage
make test

# Short tests (no race detector — faster)
make test-short

# Specific package
go test -v -race ./internal/orchestration/...

# Linux-only packages (cgroups, sandbox)
go test -v -race -run . ./internal/compute/... # runs on Linux; build-constrained on others
```

### OPA Policy Tests

OPA policy tests live alongside the policies in `configs/policies/`. The test file covers all `allow_*` rules, mobile eligibility, HTLC lifecycle, and replication factors.

**Install OPA (one-time):**

```bash
# Linux/macOS
curl -L -o /usr/local/bin/opa https://github.com/open-policy-agent/opa/releases/download/v0.68.0/opa_linux_amd64_static
chmod +x /usr/local/bin/opa

# Windows (PowerShell)
Invoke-WebRequest -Uri 'https://github.com/open-policy-agent/opa/releases/download/v0.68.0/opa_windows_amd64.exe' -OutFile "$env:USERPROFILE\go\bin\opa.exe"
```

**Run policy tests:**

```bash
# Run all 57 OPA policy tests
opa test --v1-compatible configs/policies/ -v

# Expected output: PASS: 57/57
```

> **Note:** The `--v1-compatible` flag is required because the policies use OPA v1 syntax (`if`/`contains` keywords). CI enforces this via the `opa-test` job in `.github/workflows/test.yml`.

### Test Conventions

- Table-driven tests (`[]struct{ name, input, want }`) for all pure functions
- `httptest.Server` for any test touching HTTP (Stripe, Lightning, IPFS, HTTP API)
- `//go:build linux` constraint on any test requiring Linux kernel features
- `t.Skip("requires X")` for tests that need external services (Docker, LND, IPFS daemon)
- No test may call a real external API (no real Stripe keys, no real Lightning nodes)

---

## Mobile Development

Mobile applications live in `mobile/` and are built with their respective native toolchains. The Go coordinator (this repo) must have the server-side pieces in place before mobile apps can connect.

### Go-Side Prerequisites (before starting mobile work)

These Go packages need to exist before any mobile app can function:

| Package | Needed For | Status |
|---|---|---|
| `internal/orchestration/mobile.go` | Node class taxonomy, constraint types | ✅ Completed 2026-03-02 |
| `internal/httpapi/mobilehub.go` | WebSocket hub (`/ws/nodes`) | ✅ Completed 2026-03-02 |
| `internal/httpapi/server.go` (mobile routes) | Register + list endpoints | ✅ Completed 2026-03-02 |
| `internal/orchestration/scheduler.go` (mobile) | `ScheduleMobile`, `PreemptMobileWorkload`, bandit wiring | ✅ Completed 2026-03-02 |
| `internal/orchestration/workload.go` (checkpoint) | `CheckpointData`, `SegmentIndex`, `SegmentCount` | ✅ Completed 2026-03-02 |
| `internal/wasm/executor.go` | Wasm task execution (wazero — stub compiled; real impl v0.3) | ✅ Stub completed 2026-03-02 |
| `internal/payment/htlc.go` | Hold invoice for result verification | ✅ Completed 2026-03-02 |
| `internal/notification/apns.go` | iOS push notifications | ✅ Completed 2026-03-02 |
| `configs/policies/resource_sharing.rego` (mobile) | Mobile eligibility + replication rules | ✅ Completed 2026-03-02 |

### Android TV App (`mobile/android-tv/`)

- **Language:** Kotlin
- **Build:** Android Studio / Gradle
- **Min API:** 21 (Android 5.0 — covers all current TV boxes)
- **Architecture:** WorkManager background loop; WebSocket to Go coordinator; Wasm JNI bridge
- **Testing:** Instrumented tests on Android TV emulator

### Android Smartphone App (`mobile/android/`)

- **Language:** Kotlin
- **Build:** Android Studio / Gradle
- **Min API:** 29 (Android 10 — required for `getThermalHeadroom()`)
- **Architecture:** ForegroundService + BroadcastReceiver + WorkManager; WebSocket; Wasm ARM64
- **Key constraint:** Play policy requires battery optimization exemption to be user-initiated
- **Testing:** Instrumented tests; physical device testing for thermal behavior

### iOS App (`mobile/ios/`)

- **Language:** Swift / SwiftUI
- **Build:** Xcode 16+
- **Min iOS:** 16
- **Architecture:** SwiftUI screens; WKWebView for globe; APNs for push; no background compute
- **Key constraint:** Background processing structurally prohibited by iOS — this is monitoring only
- **Testing:** XCTest; TestFlight for beta distribution

---

## ML Development

The `internal/ml` package provides pure-Go machine learning primitives with **no external ML framework dependencies**. All math (matrix inversion, UCB scoring, reward functions) is implemented in stdlib + `math`.

### Package Layout

| File | Responsibility |
|---|---|
| `internal/ml/features.go` | Dimension constants (`NodeFeatureDim`, `TaskFeatureDim`, `SystemFeatureDim`, `ContextDim`); `clamp()` helper |
| `internal/ml/bandit.go` | `LinUCBBandit` — disjoint Linear UCB; per-arm A/b/θ/AInv matrices; `Select`, `Update`, `RemoveArm` |
| `internal/ml/telemetry.go` | `TelemetryRecorder` — buffered JSONL writer; `SchedulerEvent`, `Outcome`, `RewardFor`; `EventBuilder` |
| `internal/orchestration/mlfeatures.go` | `NodeFeatures`, `TaskFeatures`, `SystemFeatures`, `BuildContext`, `SystemState` — lives in `orchestration` package to avoid import cycle |

### Import Cycle Warning

`internal/ml` must **not** import `internal/orchestration`.  Feature extraction functions reference `MobileNodeInfo` and `MobileTaskDescriptor` (defined in `orchestration`), so they live in `internal/orchestration/mlfeatures.go` instead of `internal/ml/features.go`. This breaks the cycle: `orchestration → ml` (for bandit + telemetry) without `ml → orchestration`.

### Wiring the Bandit at Runtime

```go
// In app startup (e.g., internal/app/app.go):
bandit := ml.NewLinUCBBandit(ml.ContextDim, 0.3)   // dim=20, α=0.3
rec, err := ml.NewTelemetryRecorder("data/scheduler_telemetry.jsonl", 1024)

sched.SetMLBandit(bandit)
sched.SetTelemetryRecorder(rec)
```

Pass `nil` to either setter to disable that feature — the scheduler falls back to uniform random selection and no telemetry recording.

### Recording Outcomes

When a mobile task resolves, call `RecordMobileOutcome` on the scheduler:

```go
sched.RecordMobileOutcome(
    taskID, nodeDID, string(nodeClass),
    ml.OutcomeHTLCSettle,               // or OutcomeCompleted, OutcomeError, etc.
    durationMs, maxDurationMs,
    banditCtx,                          // context vector built at dispatch time, or nil
)
```

This updates the bandit's reward model and appends a resolved `SchedulerEvent` to the telemetry JSONL file. Offline ML training pipelines can consume the JSONL directly (pandas, DuckDB, PyTorch datasets).

### Tuning α (Exploration Parameter)

| α value | Behaviour |
|---|---|
| `0.1` | Exploit — heavily favours known-good nodes |
| `0.3` | Balanced — recommended default |
| `1.0` | Explore — treats unknown nodes nearly equally |

Increase α early in deployment when few rewards have been collected. Anneal toward 0.1 as the reward model matures.

### Future ML Phases

See [`docs/research/ML_LOAD_BALANCING.md`](docs/research/ML_LOAD_BALANCING.md) for the full research report and [`ROADMAP.md`](ROADMAP.md) (v0.6.0) for the implementation timeline:

- **Phase 2** — `internal/ml/forecaster.go`: LSTM availability forecaster (Q3 2026)
- **Phase 3** — `internal/ml/graph.go`: GAT graph-aware shadow pair placement (Q4 2026)
- **Phase 4** — `internal/ml/anomaly.go`: LSTM-Autoencoder + Isolation Forest for anomaly detection (Q4 2026)

---

## Code Style

```bash
# Format
go fmt ./...

# Lint
golangci-lint run ./...

# Vet
go vet ./...
```

The golangci-lint v2 configuration runs the supported static checks and treats
`gofmt` as a formatter. Its `unused` check looks for unused declarations.
Use the separate Go [deadcode tool](https://go.dev/blog/deadcode) to check
whole-program reachability:

- `deadcode ./...` starts from production entry points and can flag a function
  that is reachable only through tests.
- `deadcode -test ./...` includes test entry points and reports functions that
  are unreachable from both production and tests.

### Commit Messages (Conventional Commits)

```
feat(orchestration): add mobile node class and WebSocket hub
fix(payment): guard against negative elapsed time in meter loop
docs(mobile): add phased integration plan
chore(deps): monthly dependency upgrade 2026-03
test(wasm): add executor unit tests
```

**Types:** `feat` · `fix` · `docs` · `style` · `refactor` · `test` · `chore`

### Pre-Commit Checklist

- [ ] `go build ./...` compiles (both with and without `-tags gui`)
- [ ] `go test -short ./...` passes
- [ ] `golangci-lint run ./...` adds no new findings compared with the target branch
- [ ] `go mod tidy` run if `go.mod` changed

---

## Pull Request Process

1. Fork → feature branch (`git checkout -b feat/my-feature`)
2. Write tests first for new packages
3. Run pre-commit checklist above
4. Open PR against `main` — CI runs tests across Linux, macOS, Windows
5. Squash merge after review approval

---

## Troubleshooting

### `package fyne.io/fyne/v2 is not in GOROOT`
```bash
go mod download
```

### `build constraints exclude all Go files`
You're building GUI code without the tag:
```bash
make build-gui  # adds -tags gui automatically
```

### `vendor/modules.txt` inconsistency
```bash
go mod vendor
```

### Tests fail after dependency update
```bash
go clean -modcache
go mod download
go mod vendor
make test-short
```

### Windows: `gcc not found`
Install MinGW: `winget install msys2` → `pacman -S mingw-w64-x86_64-gcc` → add `C:\msys64\mingw64\bin` to PATH.

---

## Resources

- [Go 1.24 Release Notes](https://go.dev/doc/go1.24)
- [Fyne Developer Docs](https://developer.fyne.io/)
- [GoReleaser Docs](https://goreleaser.com/intro/)
- [OPA / Rego Language](https://www.openpolicyagent.org/docs/latest/policy-language/)
- [IPFS Kubo HTTP API](https://docs.ipfs.tech/reference/kubo/rpc/)
- [Lightning Network (LDK)](https://lightningdevkit.org/)
- [Android Thermal API](https://developer.android.com/reference/android/os/PowerManager#getThermalHeadroom(int))
- [Android WorkManager](https://developer.android.com/topic/libraries/architecture/workmanager)
- [iOS BGTaskScheduler](https://developer.apple.com/documentation/backgroundtasks/bgtaskscheduler)
