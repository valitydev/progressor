# Progressor - Система обработки процессов

## Описание

Progressor - это высокопроизводительная система обработки долгоживущих процессов на языке Erlang/OTP. Система предназначена для управления жизненным циклом процессов, их состоянием и выполнением задач в распределенной среде с гарантиями консистентности и отказоустойчивости.

## Основные возможности

- **Управление процессами**: Создание, вызов, восстановление и удаление процессов
- **Планирование задач**: Гибкая система планирования с поддержкой таймеров и отложенного выполнения
- **Отказоустойчивость**: Автоматическое восстановление после сбоев с настраиваемой политикой повторов
- **Масштабируемость**: Поддержка пулов воркеров и горизонтального масштабирования
- **Кэширование**: Высокопроизводительное кэширование состояния процессов на основе логической репликации PostgreSQL
- **Мониторинг**: Встроенные метрики Prometheus для наблюдения за системой
- **Идемпотентность**: Защита от дублирования операций через ключи идемпотентности

## Архитектура системы

```
┌─────────────────┐    ┌──────────────────────────────────────────┐
│   Client API    │    │                Progressor                │
└─────────────────┘    │                                          │
         │              │  ┌────────────┐  ┌─────────────────────┐ │
         │              │  │ API Handler│  │     Scheduler       │ │
         ▼              │  └────────────┘  └─────────────────────┘ │
┌─────────────────┐    │                                          │
│   Thrift API    │◄───┤  ┌────────────┐  ┌─────────────────────┐ │
└─────────────────┘    │  │  Storage   │  │    Worker Pool      │ │
                       │  └────────────┘  └─────────────────────┘ │
                       │                                          │
                       │  ┌────────────┐  ┌─────────────────────┐ │
                       │  │   Cache    │  │     Processor       │ │
                       │  │   (ETS)    │  └─────────────────────┘ │
                       │  └────────────┘                          │
                       │                                          │
                       │  ┌────────────┐  ┌─────────────────────┐ │
                       │  │ PostgreSQL │  │      Scanner        │ │
                       │  └────────────┘  └─────────────────────┘ │
                       │                                          │
                       │  ┌────────────┐                          │
                       │  │  Notifier  │                          │
                       │  └────────────┘                          │
                       └──────────────────┼──────────────────────┘
                                          │
                                          ▼
                                   ┌──────────────┐
                                   │    Kafka     │
                                   └──────────────┘
```

## Основные компоненты

### 1. Процессы (Processes)
Основные сущности системы, представляющие бизнес-процессы:
- `process_id` - уникальный идентификатор процесса
- `status` - состояние процесса (`running` или `error`)
- `detail` - детальная информация о состоянии
- `aux_state` - вспомогательное состояние процесса
- `metadata` - метаданные процесса
- `history` - история событий процесса

### 2. Задачи (Tasks)
Единицы работы, выполняемые над процессами:
- `init` - инициализация нового процесса
- `call` - внешний вызов к процессу
- `repair` - восстановление процесса после ошибки
- `timeout` - обработка таймаута
- `notify` - уведомление о событии
- `remove` - удаление процесса

### 3. События (Events)
История изменений процессов:
- `event_id` - порядковый номер события
- `timestamp` - время события
- `payload` - данные события
- `metadata` - метаданные события

### 4. Воркеры (Workers)
Исполнители задач, работающие в пулах:
- Обработка задач из очередей
- Взаимодействие с процессорами
- Управление жизненным циклом задач

### 5. Сканеры (Scanners)
Компоненты поиска задач для выполнения:
- Поиск запланированных задач
- Сбор "зомби" задач
- Управление очередями выполнения

## API

### Основные операции

#### Инициализация процесса
```erlang
progressor:init(#{
    ns => 'default/default',
    id => <<"process_123">>,
    args => <<"init_args">>,
    context => <<"context_data">>,
    idempotency_key => <<"unique_key">>
}).
```

#### Вызов процесса
```erlang
progressor:call(#{
    ns => 'default/default',
    id => <<"process_123">>,
    args => <<"call_args">>,
    context => <<"context_data">>,
    idempotency_key => <<"call_key">>
}).
```

#### Восстановление процесса
```erlang
progressor:repair(#{
    ns => 'default/default',
    id => <<"process_123">>,
    args => <<"repair_args">>,
    context => <<"context_data">>
}).
```

#### Получение состояния процесса
```erlang
progressor:get(#{
    ns => 'default/default',
    id => <<"process_123">>,
    range => #{
        offset => 0,
        limit => 100,
        direction => forward
    }
}).
```

#### Добавление процесса
```erlang
progressor:put(#{
    ns => 'default/default',
    id => <<"process_123">>,
    args => #{
        process => #{
            process_id => <<"process_123">>,
            status => <<"running">>,
            aux_state => <<"state_data">>
        },
        action => #{set_timer => 1640995200}
    }
}).
```

## Конфигурация

### Основные параметры
```erlang
{progressor, [
    {defaults, #{
        storage => #{
            client => prg_pg_backend,
            options => #{
                pool => default_pool,
                scan_pool => default_scan_pool,
                front_pool => default_front_pool,
                cache => db_ref
            }
        },
        retry_policy => #{
            initial_timeout => 3,        % секунды
            backoff_coefficient => 1.2,
            max_timeout => 180,          % секунды
            max_attempts => 2,
            non_retryable_errors => [
                some_reason,
                <<"Error message">>
            ]
        },
        task_scan_timeout => 10,         % секунды
        worker_pool_size => 200,
        process_step_timeout => 30       % секунды
    }},

    {namespaces, #{
        'default/default' => #{
            processor => #{
                client => custom_processor,
                options => #{}
            },
            notifier => #{
                client => default_kafka_client,
                options => #{
                    topic => <<"events_topic">>,
                    lifecycle_topic => <<"lifecycle_topic">>
                }
            }
        }
    }}
]}
```

### Подключение к PostgreSQL
```erlang
{epg_connector, [
    {databases, #{
        progressor_db => #{
            host => "postgres",
            port => 5432,
            database => "progressor_db",
            username => "progressor",
            password => "progressor"
        }
    }},
    {pools, #{
        default_pool => #{
            database => progressor_db,
            size => 100
        },
        default_scan_pool => #{
            database => progressor_db,
            size => 1
        },
        default_front_pool => #{
            database => progressor_db,
            size => 10
        }
    }}
]}
```

### Настройка взаимодействия с Kafka
```erlang
{brod, [
    {clients, [
        {default_kafka_client, [
            {endpoints, [{"kafka1", 9092}, {"kafka2", 9092}]},
            {auto_start_producers, true},
            {default_producer_config, []}
        ]}
    ]}
]}
```

## Реализация процессора

Процессор определяет бизнес-логику обработки процессов:

```erlang
-module(my_processor).
-export([process/3]).

process({TaskType, Args, Process}, Options, Context) ->
    #{
        process_id := ProcessId,
        status := Status,
        aux_state := AuxState,
        history := History
    } = Process,

    % Бизнес-логика обработки
    NewEvent = #{
        event_id => length(History) + 1,
        timestamp => erlang:system_time(second),
        metadata => #{<<"format_version">> => 1},
        payload => create_payload(TaskType, Args)
    },

    % Результат обработки
    Result = #{
        events => [NewEvent],
        aux_state => update_aux_state(AuxState, TaskType),
        metadata => #{<<"last_update">> => erlang:system_time(second)},
        response => {ok, <<"success">>}
    },

    % Опционально установить таймер
    case should_set_timer(Process, TaskType) of
        true ->
            TimerTime = erlang:system_time(second) + 60,
            {ok, Result#{action => #{set_timer => TimerTime}}};
        false ->
            {ok, Result}
    end.
```

## Сценарии использования

### 1. Простой процесс с таймером
```erlang
% Инициализация процесса
{ok, ok} = progressor:init(#{
    ns => 'default/default',
    id => <<"timer_process">>,
    args => <<"init">>
}),

% Процессор устанавливает таймер на 60 секунд
% Через 60 секунд автоматически выполнится timeout задача

% Получение состояния
{ok, Process} = progressor:get(#{
    ns => 'default/default',
    id => <<"timer_process">>
}).
```

### 2. Внешний вызов к процессу
```erlang
% Вызов процесса с внешними данными
{ok, Response} = progressor:call(#{
    ns => 'default/default',
    id => <<"active_process">>,
    args => <<"external_data">>,
    idempotency_key => <<"call_123">>
}).
```

### 3. Восстановление после ошибки
```erlang
% Восстановление процесса в состоянии error
{ok, ok} = progressor:repair(#{
    ns => 'default/default',
    id => <<"failed_process">>,
    args => <<"repair_data">>
}).
```

## Мониторинг и метрики

Система предоставляет следующие метрики Prometheus:

### Длительность операций (гистограммы)
- `progressor_calls_scanning_duration_ms` - Время сканирования вызовов
- `progressor_timers_scanning_duration_ms` - Время сканирования таймеров
- `progressor_zombie_collection_duration_ms` - Время сборки зомби-задач
- `progressor_request_preparing_duration_ms` - Время подготовки запросов
- `progressor_task_processing_duration_ms` - Время обработки задач
- `progressor_task_completion_duration_ms` - Время завершения задач
- `progressor_process_removing_duration_ms` - Время удаления процессов
- `progressor_notification_duration_ms` - Время отправки уведомлений

### Проверка здоровья
```erlang
% Проверка состояния namespace
{Status, Details} = progressor:health_check(['default/default']).
% Status: passing | critical
```

## Развертывание

### Docker
```bash
# Сборка образа
docker build -f Dockerfile.dev -t progressor:dev .

# Запуск с docker-compose
docker-compose up -d
```

### Makefile команды
```bash
# Компиляция
make compile

# Тесты
make test

# Форматирование кода
make fmt

# Статический анализ
make dialyzer
```

## Политика повторов

Система поддерживает гибкую настройку повторов при ошибках:

```erlang
retry_policy => #{
    initial_timeout => 5,           % Начальная задержка (сек)
    backoff_coefficient => 2.0,     % Коэффициент увеличения задержки
    max_timeout => 300,             % Максимальная задержка (сек)
    max_attempts => 5,              % Максимальное количество попыток
    non_retryable_errors => [       % Ошибки без повторов
        validation_failed,
        <<"Invalid input">>
    ]
}
```

## Безопасность и лучшие практики

### Идемпотентность
- Всегда используйте ключи идемпотентности для критичных операций
- Ключи должны быть уникальными в рамках namespace

### Мониторинг
- Настройте алерты на метрики длительности операций
- Отслеживайте количество зомби-задач
- Мониторьте состояние пулов подключений к БД

### Производительность
- Настройте размеры пулов воркеров под нагрузку
- Оптимизируйте размеры пулов подключений к PostgreSQL
- Используйте партиционирование таблиц для больших объемов данных

### Отказоустойчивость
- Настройте репликацию PostgreSQL
- Используйте кластер Kafka для уведомлений
- Регулярно создавайте резервные копии данных

## Кэширование на основе логической репликации PostgreSQL

Progressor поддерживает кэширование состояния процессов с использованием логической репликации PostgreSQL. Кэш реализован на базе ETS таблиц.

### Принцип работы кэша

1. **Логическая репликация**: Используется встроенная логическая репликация PostgreSQL для отслеживания изменений в таблицах процессов и событий
2. **WAL Reader**: Компонент `epg_wal_reader` читает Write-Ahead Log (WAL) и передает изменения в кэш
3. **ETS хранилище**: Данные кэшируются в быстрых ETS таблицах в памяти Erlang
4. **Автоматическая очистка**: Неактивные процессы автоматически удаляются из кэша по таймауту

### Настройка кэширования

#### Конфигурация namespace с кэшем
```erlang
{namespaces, #{
    'cached/namespace' => #{
        storage => #{
            client => prg_pg_backend,
            options => #{
                pool => default_pool,
                cache => progressor_db  % Ссылка на базу данных
            }
        },
        processor => #{
            client => my_processor,
            options => #{}
        }
    }
}}
```

#### Инициализация кэша
```erlang
{post_init_hooks, [
    {prg_pg_cache, start, [
        #{
            progressor_db => {
                ['cached/namespace'],    % Список кэшируемых namespace
                "progressor"            % Имя приложения для replication slot
            }
        }
    ]}
]}
```

#### Настройки кэша
```erlang
% Таймаут переподключения к репликации (по умолчанию 5000 мс)
{cache_reconnect_timeout, 5000},

% Таймаут очистки неактивных процессов (по умолчанию 300000 мс = 5 мин)
{cache_cleanup_timeout, 300000}
```

### Преимущества кэширования

- **Высокая производительность**: Операции чтения выполняются из памяти без обращения к БД
- **Автоматическая синхронизация**: Изменения в БД отражаются в кэше с незначительной задержкой (задержка репликации)
- **Отказоустойчивость**: При потере соединения с репликацией кэш автоматически переподключается (при потере соединения кэш очищается до восстановления соединения)
- **Управление памятью**: Неактивные процессы автоматически удаляются из кэша

### Требования к PostgreSQL

Для работы кэширования необходимо:

1. **Логическая репликация включена**:
   ```sql
   -- В postgresql.conf
   wal_level = logical
   max_replication_slots = 10
   max_wal_senders = 10
   ```

2. **Права пользователя**:
   ```sql
   -- Пользователь должен иметь права на создание replication slot
   ALTER ROLE progressor_user REPLICATION;
   ```

3. **Publication создается автоматически** для таблиц процессов и событий кэшируемых namespace

### Мониторинг кэша

  NOT IMPLEMENTED (TODO cache_hit_counter)

## Заключение

Progressor предоставляет надежную платформу для управления долгоживущими процессами с гарантиями консистентности, отказоустойчивости и масштабируемости. Встроенное кэширование на основе логической репликации PostgreSQL обеспечивает высокую производительность операций чтения при сохранении актуальности данных. Система подходит для реализации сложных бизнес-процессов, требующих надежного управления состоянием и планирования задач.
