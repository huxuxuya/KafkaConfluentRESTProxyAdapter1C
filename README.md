[![Stars](https://img.shields.io/github/stars/huxuxuya/KafkaConfluentRESTProxyAdapter1C.svg?label=Github%20%E2%98%85&a)](https://github.com/huxuxuya/KafkaConfluentRESTProxyAdapter1C/stargazers)
[![Release](https://img.shields.io/github/tag/huxuxuya/KafkaConfluentRESTProxyAdapter1C.svg?label=Last%20release&a)](https://github.com/huxuxuya/KafkaConfluentRESTProxyAdapter1C/releases)

# KafkaConfluentRESTProxyAdapter1C
Адаптер для взаимодействия с kafka через Confluent REST Proxy.

[Документация API, на которой основан данный модуль]( https://docs.confluent.io/platform/current/kafka-rest/api.html)

[Быстро посмотреть текст модуля](./KafkaConfluentRESTProxyAdapter1C/src/CommonModules/Кафка/Module.bsl)


## Возможности
Основные возможности адаптера:
- Отправка сообщений в топики kafka
- Получение сообщений из топика kafka
- Логирование операций в рамках одного контекста(отправителя или получателя)


## Требования
- Платформа **8.3.10** и выше.
- "Коннектор: удобный HTTP-клиент для 1С:Предприятие 8" https://github.com/vbondarevsky/Connector

## Использование
Установите через поставку модуль к себе в конфигурацию(для возможности дальнейшего обновления).

## Пример использования модуля

Как отправить сообщение "Hello world!" в топик: "1с.topic":
```bsl
СоединениеКафка = Кафка.НовоеОписаниеСоединения("http://localhost:8082", "json");
Отправитель = Кафка.НовыйОтправитель(СоединениеКафка);
Кафка.ДобавитьСообщение(Отправитель, "Hello world!", "1с.topic");	   	
Кафка.ОтправитьСообщения(Отправитель);     
```

Где залогированы операции:
```bsl
ТекстЛога = СтрСоединить(Отправитель.ОписаниеСоединения.РезультатСоединения.ИсторияОпераций, Символы.ПС);
```

Как прочитать сообщения из топика: "1с.topic":
```bsl
СоединениеКафка = Кафка.НовоеОписаниеСоединения("http://localhost:8082", "json");
Подписчик = Кафка.НовыйПодписчик(СоединениеКафка, "ConsumerGroup1C", , Истина, 100);
	
Кафка.ЗарегистрироватьПодписчика(Подписчик);	
Кафка.Подписаться(Подписчик, Объект.topic);	
Сообщения = Кафка.ПолучитьСообщения(Подписчик);
Для каждого Сообщение из Сообщения Цикл 
	ТелоСообщения = Сообщение.Получить("value");
КонецЦикла;
	
Кафка.УдалитьПодисчика(Подписчик);
```
