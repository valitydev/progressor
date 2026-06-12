# Миграция action (progressor)

План доработки progressor: убрать размытую legacy-семантику (`#{set_timer => ...}`,
`unset_timer`, пересечение ключей map) **изнутри runtime** и оставить только явную
алгебру `action()` из `progressor.hrl`.

Связанный документ для hellgate: `hellgate/docs/prg-machine.md`.

---

## Принцип

**Progressor — runtime, не слой совместимости.**

- Внутри репозитория progressor после миграции **нет** map/atom legacy-action, **нет**
  `normalize/1`, **нет** dual-field (`action` + `effect`), **нет** pattern match по
  `#{set_timer := _}`.
- Процессор отдаёт в intent только `action()` из `progressor.hrl`.
- Отсутствие поля `action` в intent = `idle` (единственное допустимое «пустое» значение).
- Конвертация MG/map legacy — **на границе у потребителей** (hellgate / `prg_machine`),
  не в `prg_worker`.

Старый план с choke point `normalize(maps:get(action, ...))` **отменён**: он оставлял
старую семантику внутри progressor навсегда.

---

## Проблема (сейчас)

| Исход | Legacy в intent | Проблема |
|-------|-----------------|----------|
| unlock | `undefined` | неявно |
| suspend | `unset_timer` | atom, не в типе |
| continue сейчас | `#{set_timer => now}` | map + timestamp |
| continue позже | `#{set_timer => Ts}` | тот же map, другой смысл |
| remove сейчас | `#{remove => true}` | пересечение ключей |
| remove позже | `#{set_timer => Ts, remove => true}` | порядок клауз в `prg_worker` |

`progressor_action` выдаёт те же map/atom — не решает проблему внутри runtime.

---

## Целевое состояние

### Wire-тип `action()` (`progressor.hrl`)

```erlang
-type scheduled_action() :: timeout | remove.

-type schedule() :: #{
    at := timestamp_us(),            %% абсолютный unix us
    action := scheduled_action()     %% вид отложенной задачи
}.

-type action() ::
    idle
    | suspend
    | scheduled_action()             %% timeout | remove на top-level
    | {schedule, schedule()}.
```

Top-level `timeout` = «продолжить по timeout-задаче сразу» (legacy instant / timer 0).

Wire-значения пишутся в intent как есть, без helper-модуля:

```erlang
idle | suspend | timeout | remove
{schedule, #{at := UnixUs, action := timeout | remove}}
```

`at` — абсолютный unix us; относительное время — `erlang:system_time(microsecond) + N * 1000000`
на стороне автора. `prg_utils:to_microseconds/1` на входе runtime по-прежнему принимает sec/ms/us.

### Таблица dispatch (единственный источник правды в runtime)

| `action()` | Worker path | `task_type` |
|------------|-------------|-------------|
| `idle` (или поле отсутствует) | `success_and_unlock` | — |
| `suspend` | `success_and_suspend` | — |
| `remove` | `success_and_remove` | — |
| `timeout` | `success_and_continue` | `<<"timeout">>` (scheduled_time = now) |
| `{schedule, #{action := timeout, at := Ts}}` | `success_and_continue` | `<<"timeout">>` |
| `{schedule, #{action := remove, at := Ts}}` | `success_and_continue` | `<<"remove">>` |

Один `dispatch_action/5` — без чувствительного порядка клауз.

### `processor_intent()`

```erlang
-type processor_intent() :: #{
    events := [event()],
    action => action(),    %% отсутствие = idle
    response => term(),
    aux_state => binary(),
    metadata => map()
}.
```

Старые map/atom на поле `action` после релиза невалидны.

---

## Граница с потребителями

```
┌──────────────────────────────────────┐
│  hellgate / ff / кастомный процессор │  доменная логика
│  prg_action (hellgate, опционально)  │  timer tuple → action(); MG/repair — граница
└──────────────┬───────────────────────┘
               │  processor_intent.action :: action()
┌──────────────▼───────────────────────┐
│  progressor (runtime)                │  dispatch_action, action_to_task
│  никаких map/atom legacy             │
└──────────────────────────────────────┘
```

До обновления hellgate: `prg_machine:marshal_intent` конвертирует старые map в `action()`
**в репозитории hellgate**, не в progressor.

---

## Фаза 0. Контракт

Зафиксировать:

- таблицу dispatch выше;
- top-level `timeout` = instant continue (не путать с `task_type`);
- `at` в schedule — абсолютный unix us;
- **breaking change**: tag `vX.Y.0`, старые map/atom в intent не поддерживаются;
- список файлов progressor с legacy (grep: `set_timer`, `unset_timer`, `#{remove`).

**Критерий:** ревью контракта + согласование с hellgate по порядку релизов.

---

## Фаза 1. Типы в `progressor.hrl`

1. `scheduled_action/0`, `schedule/0`, `action/0` — wire-алгебра.
2. `processor_intent()` — `action => action()`.

**Критерий:** компилируется, dialyzer зелёный.

---

## Фаза 2. Runtime — чистый cut

Одним проходом, без transitional choke point:

1. **`prg_worker`**
   - `handle_result_success/5` → `dispatch_action(action(), ...)`.
   - Удалить case по `#{set_timer}`, `unset_timer`, `#{remove := true}`.
   - `action_to_task_type/1` — по `scheduled_action()` (`timeout | remove`).

2. **`progressor.erl`**
   - `action_to_task/3` принимает только `action()`; absent → `idle` через `maps:get/3`.

3. **Удалить** `src/progressor_action.erl`.

**Критерий:** в `src/` нет `set_timer`, `unset_timer`, `#{remove =>` (кроме комментариев/миграций БД).

---

## Фаза 3. Dogfooding и CT

В том же PR / сразу после фазы 2 — **не откладывать**:

1. `prg_echo_processor`, `benchmark/base_bench_processor` → wire `action()`.
2. Все моки в `prg_base_SUITE` → wire `action()` (не raw maps).
3. README / примеры процессора.

**Критерий:** `rebar3 ct` зелёный; grep по `test/` и `src/` не находит legacy action maps.

---

## Фаза 4. Релиз и потребители

1. CHANGELOG: breaking — формат `processor_intent.action`.
2. Migration guide **для внешних авторов**: таблица legacy → `action()` (в доке hellgate).
3. Tag `vX.Y.0`.
4. Hellgate: bump tag, `prg_action` + wire в доменах (миграция завершена, см. hellgate `docs/prg-machine.md`).

**Критерий:** progressor tag опубликован; hellgate компилируется со своим адаптером.

---

## Порядок

```
Фаза 0 → 1 → 2 + 3 (один PR) → 4
```

---

## Не делать

- `normalize/1` / dual `action`+`effect` **внутри progressor**.
- Отдельный модуль-обёртка только ради типов — типы в `progressor.hrl`.
- «Зелёный CT без смены моков» — откладывает legacy внутри репозитория.
- Deprecated map-типы в `progressor.hrl` «на несколько фаз».
- `{set_timer, #{...}}` как wire-формат — переносит путаницу в кортежи.
- Authoring-типы (`{timeout, N}`) в `processor_intent` — только `at` / `timeout` / `{schedule, ...}`.
- Доменный аккумулятор hellgate (`set_timeout(0, Action)` по шагам) в progressor.

---

## Чеклист «миграция завершена»

- [x] типы `action/0`, `schedule/0` в `progressor.hrl`
- [x] `prg_worker` — только `dispatch_action/5` по `action()`
- [x] `progressor.erl` — только `action()` в `action_to_task`
- [x] `progressor_action.erl` удалён
- [x] grep `set_timer|unset_timer` в `src/` и `test/` — пусто
- [x] CT зелёный (`make wdeps-test`)
- [ ] CHANGELOG + tag
- [x] hellgate: wire `action()`, `prg_action`, CI green (до tag bump)
