wb-mqtt-db
==========

Сервис для сохранения данных в БД sqlite. Сохраняет значения mqtt каналов устройства, которые указаны в конфигурации.

Соглашения о наименованиях в MQTT: https://github.com/wirenboard/conventions

Конфигурация демона сохранения
------------------------------

В примере демон сохраняет данные от устройств “kvadro-1wire_69" и "wb-w1" (драйвера встроенных портов 1-wire). 
Данные от устройства “kvadro-1wire_42” из конфига выше не сохраняются.

```
root@wirenboard:~# cat /etc/wb-mqtt-db.conf
```

```jsonc
{
  // Список групп
  "groups": [
    {
      // Название группы
      "name": "w1",

      // Список каналов, относящихся к группе.
      // Формат: ИМЯ_УСТРОЙСТВА/ИМЯ_КАНАЛА.
      // '+' означает, что надо использовать все устройства или каналы.
      // Например: +/+      - все каналы всех устройств
      //           wb-w1/+  - все каналы устройства wb-w1
      "channels" : ["kvadro-1wire_69/+", "wb-w1/+"],

      // Максимальное число записей одного канала в базе.
      // При превышении, наиболее старые записи будут удалены.
      "values" : 10000,

      // Максимальное число записей всех каналов, относящихся к группе.
      // При превышении, наиболее старые записи будут удалены.
      "values_total" : 100000,

      // Минимальный период записи данных в базу по одному каналу в секундах.
      // Если данные приходят чаще, они будут усреднены.
      // В базу попадёт одна запись за указанный промежуток, 
      // содержащая минимальное, максимальное и среднее значения канала.
      "min_interval" : 5,

      // Минимальный период записи опорных точек в базу по одному каналу в секундах.
      // Опорные точки пишутся, если были получены сообщения с неизменившимися данными.
      "min_unchanged_interval" : 3600,

      // Максимальное число внеочередных записей.
      // Количество внеочерендых записей определяется предыдущим временем, 
      // в течение которого не было сообщений.
      // За каждый такой промежуток, больший чем минимальный интервал записи, 
      // даётся возможность сделать одну внеочередную запись.
      // Внеочередные записи будут записываться сразу по получению, 
      // без учёта минимального периода записи.
      "max_burst": 10
    }
  ],

  // Путь до файла базы данных
  "database" : "/var/lib/wirenboard/db/data.db",

  // Максимальное время в секундах обработки запроса получения данных из базы
  "request_timeout": 9,

  // Включение выдачи отладочных сообщений
  "debug": false
}
```

Внеочередная запись
-------------

Настройка внеочередной записи полезна в тех случаях, когда параметр в целом меняется редко, но бывают ситуации,
когда происходит несколько последовательных изменений в короткий срок.
Например, при контроле открытия и закрытия дверей.

В процессе работы `wb-mqtt-db` подсчитывает число записей, если в течение `min_interval` не было сделано ни одной записи,
разрешается сделать одну внеочередную. Если в течение следующего `min_interval` тоже не было записей,
то можно сделать ещё одну внеочередную запись и т.д.

Максимальное число внеочередных записей ограничено параметром `max_burst`.
При получении нового сообщения по MQTT, будет сделана внеочередая запись, число доступных записей уменьшится на 1.
Так будет происходить пока оно не станет равным нулю.
Последующие записи будут делаться согласно настройкам, раз в `min_interval`, будут записаны минимальное,
максимальное и среднее значение за интервал.

Запросы MQTT RPC
================
Для получения данных о каналах и массива значений каналов используется протокол [MQTT-RPC](https://github.com/wirenboard/mqtt-rpc).

`wb-mqtt-db` предоставляет RPC драйвер `db_logger`, который содержит RPC сервис `history`.

Публикация запросов отправляется в топик:
```
/rpc/v1/db_logger/history/<RPC-method>/<client_id>
```
Где:
* `RPC-method` - имя метода сервиса,
* `client_id` - ID клиента сервиса.

Ответы публикуются в топик:
```
/rpc/v1/db_logger/history/<RPC-method>/<client_id>/reply
```

Методы сервиса
==============

get\_channels
-------------

Запрос возвращает список всех регистрируемых каналов.

### Входные параметры

В поле `params` пустой JSON объект:
```jsonc
{ "id": <ID транзакции>, "params": {} }
```

### Возвращаемое значение

В поле `result` JSON-объект со следующими полями:

* *channels* - ассоциативный массив, где ключи - имена каналов в формате "device/control", значения - объекты:
  * *items* - количество зарегистрированных значений канала;
  * *last\_ts* - временная метка (UNIX timestamp UTC) последнего зарегистрированного значения.

Пример ответа:
```jsonc
{
  "error": null,
  // ID транзакции
  "id": "1234",
  "result": {
    // Aссоциативный массив, где ключи - имена каналов в формате "device/control", значения - объекты
    "channels": {
      "buzzer/enabled": {
        // Количество зарегистрированных значений канала
        "items": 29,
        // Временная метка (UNIX timestamp UTC) последнего зарегистрированного значения
        "last_ts": 1665126529
      },
      "buzzer/frequency": {
        "items": 29,
        "last_ts": 1665126529
      },
      "buzzer/volume": {
        "items": 29,
        "last_ts": 1665126529
      },
      ...
    }
  }
}
```

get\_values
-----------

Запрос возвращает последовательность зарегистрированных значений для канала/каналов.

### Входные параметры

В поле `params` JSON-объект со следующими полями:

* *ver* - версия запроса (int): 0 - полные имена полей ответа, 1 - короткие (для экономии трафика). По умолчанию - 0;
* *channels* - список каналов - массив из пар \[device, control\] (например, \[\["dev1, "chan1"], \["dev1", "chan2"], \["dev2", "chan1"]). Обязательный параметр;
* *timestamp* - временной интервал - объект с двумя полями - временными метками (UNIX timestamp UTC):
  * *gt* - начало интервала, по умолчанию - 0;
  * *lt* - конец интервала, по умолчанию - текущее время;
* *uid* - интервал ID записей - объект с полем *gt* - минимальный ID записи;
* *limit* - максимальное количество записей в ответе;
* *min_interval* - минимальный интервал между записями в ответе (в мс);
* *max_records* - максимальное число записей в заданном интервале. Если число записей в базе для запрошенного интервала больше этого параметра, в ответе будут переданы усреднённые значения. Данный параметр имеет более высокий приоритет чем *min_interval*.
* *request_timeout* - максимальное время выполнения запроса (в секундах). Если за указанное время запрос не будет выполнен, вернётся ошибка "Request timeout" (код -32100, не путать с "MQTT request timeout", возникающей при потере соединения с сервисом);
* *with_milliseconds* - присылать в ответе на запрос время с точностью до миллисекунд (true/false).

### Возвращаемое значение

В поле `result` JSON-объект со следующими полями:

* *values* - массив значений. Значение - JSON-объект, в зависимости от параметра *ver*:
  * ver == 0:
    * *uid* - ID записи;
    * *device* - имя устройства;
    * *control* - имя канала;
    * *timestamp* - временная метка значения (UNIX timestamp UTC);
    * *min* - минимальное значение за интервал;
    * *max* - максимальное значение за интервал;
    * *value* - (среднее) значение за интервал;
    * *retain* - retain-флаг для сообщения;
  * ver == 1:
    * *c* - внутренний ID канала (для пары device/control);
    * *i* == *uid*;
    * *v* == *value*;
    * *t* - временная метка значения (UNIX timestamp UTC). Если в запросе задан параметр *with_milliseconds*, то передаётся число с плавающей запятой, где дробная часть - мс;
    * *min*, *max*, *retain*
* *has_more* - если true, то в базе остались значения для данных каналов, не попавшие в ответ (например, отсеченные по *limit*).
