# yougpu-agent

In-VM agent для YouGPU GPU-instance'ов. Реконсилит state против declarative spec от backend'а:
- Маунтит/анмаунтит S3 storage drives через rclone systemd units.
- Ротирует STS-credentials для S3 каждые 12 часов.
- При получении `lifecycle.deletion_requested_at` — графно останавливает контейнеры, флашит rclone VFS-кэш, репортит `synced` и ждёт пока backend снесёт VM.

Скачивается cloud-init'ом при первом boot'е, запускается под systemd как `yougpu-agent.service`. Контракт API описан в [../yougpu-backend/AGENT_DECLARATIVE_PROTOCOL.md](../yougpu-backend/AGENT_DECLARATIVE_PROTOCOL.md).

## Локальная сборка

```bash
make build              # → bin/yougpu-agent (host OS)
make build-linux        # → bin/yougpu-agent-linux-amd64 (cross-compile)
make test               # unit-тесты
make vet                # go vet
```

## Запуск (на VM)

Конфиг через env vars + файл с токеном:

| Переменная | Значение |
|---|---|
| `YOUGPU_BACKEND_URL` | `https://api.yougpu.ru/instances/<id>/agent` (включает basepath) |
| `YOUGPU_TOKEN_FILE` | `/var/lib/yougpu/provisioning_token` (default) |
| `YOUGPU_STATE_DIR` | `/var/lib/yougpu` (default) |

Токен cloud-init пишет в `YOUGPU_TOKEN_FILE` mode `0400`, агент читает при старте.

## Релизы

Tag `vX.Y.Z` → CI (GitHub Actions) cross-compile'ит `linux-amd64` и публикует в GitHub Releases:

```
https://github.com/bogdanaks/yougpu-agent/releases/download/vX.Y.Z/yougpu-agent-linux-amd64
```

Backend подставляет нужную версию в cloud-init через env-переменные `AGENT_DEFAULT_VERSION` и `AGENT_RELEASES_URL` (по умолчанию — URL GitHub-релизов выше). Старые VM продолжают работать на своей версии — обновление случается при пересоздании. Если в будущем переедем на S3/CDN — менять только env, формат пути совместим (`<base>/<tag>/yougpu-agent-linux-amd64`).

## Структура

```
cmd/yougpu-agent/    # entrypoint
internal/
  agent/             # главный цикл: poll spec → reconcile → post status
  client/            # HTTP-клиент к /agent/{spec,status,rotate-storage-keys,provisioning-status}
  config/            # env + token loading
  disk/              # rclone systemd unit generation, mount/unmount
  lifecycle/         # state machine alive → syncing → synced → shutdown
  reconcile/         # чистая функция (spec, observed) → []Action
  sts/               # периодическая ротация S3 creds
  system/            # тонкие обёртки вокруг systemctl/exec
deploy/
  yougpu-agent.service  # systemd unit, ставится cloud-init'ом
```
