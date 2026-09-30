# Releasing

Релиз создаётся **push'ем тега** вида `v*` (например `v0.1.0`). Workflow `release.yml`
триггерится на тег, собирает `linux/amd64` и публикует бинарь в GitHub Release.

URL итогового бинаря (immutable, не меняется после публикации):

```
https://github.com/bogdanaks/yougpu-agent/releases/download/<tag>/yougpu-agent-linux-amd64
```

## Версионирование

Semver `vMAJOR.MINOR.PATCH`:

- **PATCH** (`v0.1.0` → `v0.1.1`) — bugfix без изменения поведения spec/status контракта.
- **MINOR** (`v0.1.x` → `v0.2.0`) — новая фича агента, обратно совместимая.
- **MAJOR** (`v0.x.x` → `v1.0.0`) — breaking change в контракте с backend (новые обязательные поля spec, изменение URL endpoint'ов и т.п.). Требует синхронной обновки backend.

## Шаги выпуска

1. Убедиться что `master` зелёный: `make test && make vet`.
2. Создать тег на текущий HEAD:
   ```bash
   git tag v0.1.1
   git push origin v0.1.1
   ```
3. Открыть Actions tab — workflow `Release` должен пройти за ~1 минуту. На выходе появится новый GitHub Release с двумя файлами:
   - `yougpu-agent-linux-amd64`
   - `yougpu-agent-linux-amd64.sha256`
4. Проверить что бинарь доступен:
   ```bash
   curl -fsSL -I https://github.com/bogdanaks/yougpu-agent/releases/download/v0.1.1/yougpu-agent-linux-amd64
   ```
   Ожидается `HTTP/2 200` (или 302 редирект на S3 GitHub'а).
5. Обновить backend env: `AGENT_DEFAULT_VERSION=v0.1.1` → задеплоить.
6. Новые VM создаются уже с новым бинарём. Старые VM продолжают работать на своей версии до пересоздания (агент не self-update'ится).

## Чего НЕ делать

- **Не перезаписывать опубликованные теги** (`git tag -f v0.1.0 && git push -f origin v0.1.0`). GitHub Release перезатрётся, но:
  - VM которые уже скачали старый бинарь — на нём и останутся.
  - Новые VM получат новую версию без явного бампа в backend env — труднее дебажить.
  - Нельзя откатиться (предыдущий бинарь потерян).
  - Нарушается immutability — нельзя по логам понять "что именно было в проде".

  Вместо force-push тега — выпусти PATCH (`v0.1.1`).

- **Не пушить код в `master` без тега** в надежде что "релиз сам случится". Workflow `release.yml` триггерится **только** на push тегов `v*`. Push в `master` запускает `ci.yml` (тесты + temp artifact на 90 дней), но GitHub Release не создаёт — URL для скачивания остаётся со старым бинарём.

- **Не использовать теги без префикса `v`** (`1.0.0`, `release-1`). Workflow фильтрует по паттерну `v*`.

## Hotfix flow

1. Branch from `master`: `git switch -c hotfix/x`.
2. Fix + tests.
3. PR + merge в `master`.
4. Тег: `git tag v0.1.2 && git push origin v0.1.2`.
5. Bump `AGENT_DEFAULT_VERSION` сразу на prod (минуя staging), если инцидент критичный.

## Откат

Откатить = указать в backend env предыдущую версию.

```bash
# Откат с v0.1.1 на v0.1.0:
# 1. В backend env: AGENT_DEFAULT_VERSION=v0.1.0
# 2. Передеплоить backend.
# 3. Новые VM пойдут с v0.1.0. Старые VM на v0.1.1 продолжат работать.
```

Если в v0.1.1 был баг ломающий уже-запущенные VM — пересоздать их (это уже не откат версии агента, это recovery инстансов).

## Локальная сборка

```bash
make build               # bin/yougpu-agent (хост-ОС)
make build-linux         # bin/yougpu-agent-linux-amd64
make test                # go test ./...
make vet                 # go vet ./...
VERSION=v0.0.0-test make build-linux  # вшить кастомную версию
```

Версия вшивается в бинарь через `-ldflags "-X main.version=$(VERSION)"` и доступна как `yougpu-agent --version`.
