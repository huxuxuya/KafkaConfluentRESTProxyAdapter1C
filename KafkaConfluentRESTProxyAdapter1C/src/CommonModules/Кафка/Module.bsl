#Область ПрограммныйИнтерфейс

// Новое описание соединения. создает описание соединения, из данных которого в дальнейшем будет инициализировано 
// соединение, в котором так же хранится кеш соединения и лог выполненных операций.
// 
// Параметры:
//  БазовыйАдрес - Строка - Адрес подключения к rest proxy, например "http://localhost:8082"
//  ФорматОбмена - Строка - Формат обмена, в котором должен происходить обмен с rest proxy, 
//  пока единственный поддерживемый это "json".
// 
// Возвращаемое значение:
//  Структура - Описание соединения:
// * Адрес - Строка - Строка базового адреса подключения к rest proxy 
// * Формат - Строка - Формат обмена.
// * Заголовки - Соответствие - Заголовки необходимые для соединения с rest proxy, зависят от формата и операции.
// * КешСоединения - Неопределено, HTTPСоединение - кеш соединения, если подключение уже выполнялось.
// * РезультатСоединения - Структура -:
// ** ИсторияОпераций - Массив - История выполененных операций в рамках данного описания соединения.
// ** ИнформацияОбОперации - Строка - Текстовое представление информации о последней выполненной операции.
// ** ОшибкаВыполнения - Булево - Флаг наличия ошибки, после выполнения операции.
Функция НовоеОписаниеСоединения(БазовыйАдрес, ФорматОбмена = "json") Экспорт 
	
	ВозможныеФорматы = Новый Массив;
	ВозможныеФорматы.Добавить("json");
	ВозможныеФорматы.Добавить("binary");
	
	МассивОпераций = Новый Массив();
	РезультатВыполнения = Новый Структура;
	РезультатВыполнения.Вставить("ИсторияОпераций", МассивОпераций);
	РезультатВыполнения.Вставить("ИнформацияОбОперации", Неопределено);
	РезультатВыполнения.Вставить("ОшибкаВыполнения", Ложь);

	СвойстваСоединения = Новый Структура;
	СвойстваСоединения.Вставить("Адрес", БазовыйАдрес);
	СвойстваСоединения.Вставить("Формат", ФорматОбмена);
	СвойстваСоединения.Вставить("Заголовки", Неопределено);
	СвойстваСоединения.Вставить("КешСоединения", Неопределено);
	СвойстваСоединения.Вставить("РезультатСоединения", РезультатВыполнения);
	
	ВыполнитьКонтрольДопустимыхФорматов(СвойстваСоединения, ВозможныеФорматы);
	
	ЗаполнитьЗаголовки(СвойстваСоединения, ФорматОбмена);
	
	Возврат СвойстваСоединения; 
	
КонецФункции

// Новый отправитель. Данная функция создает описание структуры отправителя, через которую должны выполняться 
// дальнейшие операции взаимодействия с rest proxy.
// 
// Параметры:
//  ОписаниеСоединения - См. НовоеОписаниеСоединения
// 
// Возвращаемое значение:
//  Структура - Новый отправитель:
// * ОписаниеСоединения - См. НовоеОписаниеСоединения
// * СообщенияПоТопикам - Соответствие - Топики, по которым должны отправляться сообщения.
Функция НовыйОтправитель(ОписаниеСоединения) Экспорт
	
	ОписаниеОтправителя = СвойстваОтправителя();
	ОписаниеОтправителя.ОписаниеСоединения = ОписаниеСоединения;

	ВыполнитьЛогированиеНовыйОтправитель(ОписаниеСоединения);
		
	Возврат ОписаниеОтправителя;
	
КонецФункции
   
Функция НовыйПодписчик( ОписаниеСоединения, 
						ИмяГруппыПодписчиков = Неопределено, 
						ИмяЭкземпляраПодписчика = Неопределено, 
						ИспользоватьАвтоподтверждение = Ложь,
						МаксимальныйРазмерОтветаБайт = 100) Экспорт
	
	СгенерироватьИмяПустомуЗначению(ИмяГруппыПодписчиков);
	СгенерироватьИмяПустомуЗначению(ИмяЭкземпляраПодписчика);
		
	ОписаниеПодписчика = СвойстваПодписчика();
	ОписаниеПодписчика.ОписаниеСоединения = ОписаниеСоединения;
	ОписаниеПодписчика.ИмяГруппыПодписчиков = ИмяГруппыПодписчиков;
	ОписаниеПодписчика.ИмяЭкземпляраПодписчика = ИмяЭкземпляраПодписчика;
	ОписаниеПодписчика.ИспользоватьАвтоподтверждение = ИспользоватьАвтоподтверждение;
	ОписаниеПодписчика.МаксимальныйРазмерОтветаБайт = МаксимальныйРазмерОтветаБайт;

	ВыполнитьЛогированиеНовыйПодписчик(ОписаниеСоединения); 	
	
	Возврат ОписаниеПодписчика;
	
КонецФункции

Процедура ЗарегистрироватьПодписчика(Подписчик) Экспорт 
	
	ОписаниеСоединения = Подписчик.ОписаниеСоединения;    	
	Заголовки = ЗаголовкиСоединения(ОписаниеСоединения);
     	
	МассивАдреса = Новый Массив;
	МассивАдреса.Добавить(ОписаниеСоединения.Адрес);
	МассивАдреса.Добавить("consumers");
	МассивАдреса.Добавить(Подписчик.ИмяГруппыПодписчиков);       
	Адрес = СтрСоединить(МассивАдреса, "/");      
		
	ПараметрыПодписчика = Новый Соответствие;                        
	ПараметрыПодписчика.Вставить("name", Подписчик.ИмяЭкземпляраПодписчика);
	ПараметрыПодписчика.Вставить("format", "json");               
	ПараметрыПодписчика.Вставить("auto.offset.reset", "earliest"); 

	Если  Не Подписчик.ИспользоватьАвтоподтверждение Тогда
		ПараметрыПодписчика.Вставить("auto.commit.enable", "false");
	КонецЕсли; 
	
	ТелоЗапроса = КоннекторHTTP.ОбъектВJson(ПараметрыПодписчика);
	
	Если  Не Подписчик.ИспользоватьАвтоподтверждение Тогда
 	   ТелоЗапроса = СтрЗаменить(ТелоЗапроса, "auto_commit_enable", "auto.commit.enable"); 
	КонецЕсли;
	
	ТелоЗапроса = СтрЗаменить(ТелоЗапроса, "auto_offset_reset", "auto.offset.reset"); 	

	Ответ = КоннекторHTTP.Post(Адрес, ТелоЗапроса, Заголовки);      

	ВыполнитьЛогированиеЗарегистрироватьПодписчика(Подписчик, "Post", ТелоЗапроса, Ответ);
	
КонецПроцедуры

Функция ПолучитьСдвиги(Подписчик) Экспорт 
	
	ОписаниеСоединения = Подписчик.ОписаниеСоединения;    	
	Заголовки = ЗаголовкиСоединения(ОписаниеСоединения);
	
	МассивАдреса = Новый Массив;
	МассивАдреса.Добавить(ОписаниеСоединения.Адрес);
	МассивАдреса.Добавить("consumers");
	МассивАдреса.Добавить(Подписчик.ИмяГруппыПодписчиков);  
	МассивАдреса.Добавить("instances");
	МассивАдреса.Добавить(Подписчик.ИмяЭкземпляраПодписчика);
	МассивАдреса.Добавить("offsets");   

	Адрес = СтрСоединить(МассивАдреса, "/");
	
	ОтветЗапроса = КоннекторHTTP.Get(Адрес, , Заголовки);
	
	Сдвиги = Новый Массив;
	Если ОтветЗапроса.КодСостояния = 200 Тогда
		
		ПолученныеДанные = КоннекторHTTP.КакJson(ОтветЗапроса);
		Если ПолученныеДанные.Количество() > 0 Тогда
			
			Для каждого ЗаписьДанных Из ПолученныеДанные Цикл
				
				Сдвиги.Добавить(ЗаписьДанных);
						
			КонецЦикла;
			
		КонецЕсли;  		
	КонецЕсли;
	
	ВыполнитьЛогированиеПолучитьСдвиги(Подписчик, "Get", "<empty>", ОтветЗапроса);
	
	Возврат Сдвиги;

КонецФункции

Функция ПолучитьСообщения(Подписчик) Экспорт 
	
	ОписаниеСоединения = Подписчик.ОписаниеСоединения;    	
	Заголовки = ЗаголовкиСоединения(ОписаниеСоединения);
	
	МассивАдреса = Новый Массив;
	МассивАдреса.Добавить(ОписаниеСоединения.Адрес);
	МассивАдреса.Добавить("consumers");
	МассивАдреса.Добавить(Подписчик.ИмяГруппыПодписчиков);  
	МассивАдреса.Добавить("instances");
	МассивАдреса.Добавить(Подписчик.ИмяЭкземпляраПодписчика);
	МассивАдреса.Добавить("records");   

	Адрес = СтрСоединить(МассивАдреса, "/");
	
	ПараметрыЗапроса = Новый Структура;
	Если ЗначениеЗаполнено(Подписчик.МаксимальныйРазмерОтветаБайт) Тогда
		ПараметрыЗапроса.Вставить("max_bytes", Подписчик.МаксимальныйРазмерОтветаБайт);
	КонецЕсли;

	ОтветЗапроса = КоннекторHTTP.Get(Адрес, ПараметрыЗапроса, Заголовки);
	
	МиниммальныйСдвиг = Неопределено;
	МаксимальныйСдвиг = Неопределено;
	Сообщения = Новый Массив;
	Если ОтветЗапроса.КодСостояния = 200 Тогда
		
		ПолученныеДанные = КоннекторHTTP.КакJson(ОтветЗапроса);
		Если ПолученныеДанные.Количество() > 0 Тогда
			
			ДатаСобытия = ТекущаяДата();
			
			Для каждого ЗаписьДанных Из ПолученныеДанные Цикл
				
				Сообщения.Добавить(ЗаписьДанных);
				
				Партиция = ЗаписьДанных["partition"];
				Сдвиг = ЗаписьДанных["offset"]; 
				
				МиниммальныйСдвиг = ?(МиниммальныйСдвиг = Неопределено, Сдвиг, МиниммальныйСдвиг);
				МаксимальныйСдвиг = ?(МаксимальныйСдвиг = Неопределено, Сдвиг, МаксимальныйСдвиг);
				
				МиниммальныйСдвиг = Мин(МиниммальныйСдвиг,Сдвиг);
				МаксимальныйСдвиг = Макс(МаксимальныйСдвиг, Сдвиг);;
			
			КонецЦикла;			
		КонецЕсли;      		
	КонецЕсли;
	
	ВыполнитьЛогированиеПолучитьСообщения(Подписчик, "Get", ОтветЗапроса, МиниммальныйСдвиг, МаксимальныйСдвиг);
	
	Возврат Сообщения;

КонецФункции

Процедура ПодтвердитьПолучение(Подписчик, СвойстваПодтверждения) Экспорт 
	
    ОписаниеСоединения = Подписчик.ОписаниеСоединения;    	
    Заголовки = ЗаголовкиСоединения(ОписаниеСоединения);
    
    МассивАдреса = Новый Массив;
    МассивАдреса.Добавить(ОписаниеСоединения.Адрес);
    МассивАдреса.Добавить("consumers");
    МассивАдреса.Добавить(Подписчик.ИмяГруппыПодписчиков);  
    МассивАдреса.Добавить("instances");
    МассивАдреса.Добавить(Подписчик.ИмяЭкземпляраПодписчика);
    МассивАдреса.Добавить("offsets");

    Адрес = СтрСоединить(МассивАдреса, "/");

    
    Оффсет = Новый Структура; 
    Оффсет.Вставить("topic", СвойстваПодтверждения.Топик);
    Оффсет.Вставить("partition", СвойстваПодтверждения.Партиция);
    Оффсет.Вставить("offset", СвойстваПодтверждения.Сдвиг);
    
    МассивОффсетов = Новый Массив;
    МассивОффсетов.Добавить(Оффсет);
    
    ПараметрОффсетов = Новый Структура;
    ПараметрОффсетов.Вставить("offsets", МассивОффсетов);
    
    ПараметрОффсетов = КоннекторHTTP.ОбъектВJson(ПараметрОффсетов);
    
    РезультатПодтверждениеОффсета = КоннекторHTTP.Post(Адрес, ПараметрОффсетов, Заголовки);    
	
	ВыполнитьЛогированиеПодтвердитьПолучение(Подписчик, "Post", ПараметрОффсетов, РезультатПодтверждениеОффсета);

КонецПроцедуры

Процедура ПерезаписатьСдвиг(Подписчик, СвойстваСдвига) Экспорт 
	
    ОписаниеСоединения = Подписчик.ОписаниеСоединения;    	
    Заголовки = ЗаголовкиСоединения(ОписаниеСоединения);
    
    МассивАдреса = Новый Массив;
    МассивАдреса.Добавить(ОписаниеСоединения.Адрес);
    МассивАдреса.Добавить("consumers");
    МассивАдреса.Добавить(Подписчик.ИмяГруппыПодписчиков);  
    МассивАдреса.Добавить("instances");
    МассивАдреса.Добавить(Подписчик.ИмяЭкземпляраПодписчика);
    МассивАдреса.Добавить("positions");

    Адрес = СтрСоединить(МассивАдреса, "/");

    
    Оффсет = Новый Структура; 
    Оффсет.Вставить("topic", СвойстваСдвига.Топик);
    Оффсет.Вставить("partition", СвойстваСдвига.Партиция);
    Оффсет.Вставить("offset", СвойстваСдвига.Сдвиг);
    
    МассивОффсетов = Новый Массив;
    МассивОффсетов.Добавить(Оффсет);
    
    ПараметрОффсетов = Новый Структура;
    ПараметрОффсетов.Вставить("offsets", МассивОффсетов);
    
    ПараметрОффсетов = КоннекторHTTP.ОбъектВJson(ПараметрОффсетов);
    
    РезультатПодтверждениеОффсета = КоннекторHTTP.Post(Адрес, ПараметрОффсетов, Заголовки);    
	
	ВыполнитьЛогированиеПерезаписатьСдвиг(Подписчик, "Post", ПараметрОффсетов, РезультатПодтверждениеОффсета);

КонецПроцедуры

Процедура НазначитьПолучателюТопикИРаздел(Подписчик, Топик, Раздел) Экспорт 
	
    ОписаниеСоединения = Подписчик.ОписаниеСоединения;    	
    Заголовки = ЗаголовкиСоединения(ОписаниеСоединения);
    
    МассивАдреса = Новый Массив;
    МассивАдреса.Добавить(ОписаниеСоединения.Адрес);
    МассивАдреса.Добавить("consumers");
    МассивАдреса.Добавить(Подписчик.ИмяГруппыПодписчиков);  
    МассивАдреса.Добавить("instances");
    МассивАдреса.Добавить(Подписчик.ИмяЭкземпляраПодписчика);
    МассивАдреса.Добавить("assignments");

    Адрес = СтрСоединить(МассивАдреса, "/");

    
    ТопикИРаздел = Новый Структура; 
    ТопикИРаздел.Вставить("topic", Топик);
    ТопикИРаздел.Вставить("partition", Раздел);
    
    МассивТопиковИРазделов = Новый Массив;
    МассивТопиковИРазделов.Добавить(ТопикИРаздел);
    
    ПараметрОффсетов = Новый Структура;
    ПараметрОффсетов.Вставить("partitions", МассивТопиковИРазделов);
    
    ТелоЗапроса = КоннекторHTTP.ОбъектВJson(ПараметрОффсетов);
    
    РезультатПодтверждениеОффсета = КоннекторHTTP.Post(Адрес, ТелоЗапроса, Заголовки);    
	
	ВыполнитьЛогированиеНазначитьПолучателюТопикИРаздел(Подписчик, "Post", ТелоЗапроса, РезультатПодтверждениеОффсета);

КонецПроцедуры

Процедура УдалитьПодисчика(Подписчик) Экспорт 
	
	ОписаниеСоединения = Подписчик.ОписаниеСоединения;    	
	Заголовки = ЗаголовкиСоединения(ОписаниеСоединения);
	
	МассивАдреса = Новый Массив;
	МассивАдреса.Добавить(ОписаниеСоединения.Адрес);
	МассивАдреса.Добавить("consumers");
	МассивАдреса.Добавить(Подписчик.ИмяГруппыПодписчиков);  
	МассивАдреса.Добавить("instances");
	МассивАдреса.Добавить(Подписчик.ИмяЭкземпляраПодписчика);

	Адрес = СтрСоединить(МассивАдреса, "/");
     
    // Удаление подписчика
    РезультатУдаления =  КоннекторHTTP.Delete(Адрес, , Заголовки);  
	
	ВыполнитьЛогированиеУдалитьПодисчика(Подписчик, "Delete", РезультатУдаления);

КонецПроцедуры

Процедура Подписаться(Подписчик, Топик) Экспорт 

	ОписаниеСоединения = Подписчик.ОписаниеСоединения;
    Заголовки = ЗаголовкиСоединения(ОписаниеСоединения);
	
	МассивАдреса = Новый Массив;
	МассивАдреса.Добавить(ОписаниеСоединения.Адрес);
	МассивАдреса.Добавить("consumers");
	МассивАдреса.Добавить(Подписчик.ИмяГруппыПодписчиков);  
	МассивАдреса.Добавить("instances");
	МассивАдреса.Добавить(Подписчик.ИмяЭкземпляраПодписчика);
	МассивАдреса.Добавить("subscription");   

	Адрес = СтрСоединить(МассивАдреса, "/");
	ТемаПодписки = Новый Массив();
	ТемаПодписки.Добавить(Топик);
	
	ТопикиПодписки = Новый Структура;
	ТопикиПодписки.Вставить("topics", ТемаПодписки);
	
	ТелоЗапросаПодписки = КоннекторHTTP.ОбъектВJson(ТопикиПодписки);

 	РезультатПодписки = КоннекторHTTP.Post(Адрес, ТелоЗапросаПодписки, Заголовки); 
	
 	ВыполнитьЛогированиеПодписаться(Подписчик, "Post", ТелоЗапросаПодписки, РезультатПодписки);

КонецПроцедуры

Процедура ОтправитьСообщения(Отправитель) Экспорт

	ОписаниеСоединения = Отправитель.ОписаниеСоединения;
	Заголовки = ЗаголовкиСоединения(ОписаниеСоединения, Истина, Ложь);
	
	
	МассивАдреса = Новый Массив;
	МассивАдреса.Добавить(ОписаниеСоединения.Адрес);
	МассивАдреса.Добавить("topics");
	Адрес = СтрСоединить(МассивАдреса, "/");
	                                                        
	СообщенияПоТопикам = Отправитель.СообщенияПоТопикам;	
	Для Каждого ТопикСообщения Из СообщенияПоТопикам Цикл
		Топик = ТопикСообщения.Ключ;
		Сообщения = ТопикСообщения.Значение;
		АдресТопика = Адрес + "/" + Топик;
		                                                       
		СоответствиеСообщений = Новый Структура("records", Сообщения);
		СообщенияПодготовленные = КоннекторHTTP.ОбъектВJson(СоответствиеСообщений);
						
		РезультатОтправкиСообщения = КоннекторHTTP.Post(АдресТопика, СообщенияПодготовленные, Заголовки);    
		
		ВыполнитьЛогированиеОтправитьСообщения(Отправитель, "Post", СообщенияПодготовленные, РезультатОтправкиСообщения);
	
	КонецЦикла;
	
КонецПроцедуры

Процедура ДобавитьСообщение(Отправитель, Сообщение, Топик, Ключ = Неопределено, Раздел = Неопределено) Экспорт
		
	НовоеСообщение = "";	
	Если Отправитель.ОписаниеСоединения.Формат = "json" Тогда 
		
		ПараметрыСообщения = Новый Структура;    
		
		Если ЗначениеЗаполнено(Раздел) Тогда
			ПараметрыСообщения.Вставить("partition", Раздел);  
		КонецЕсли;
		
		Если ЗначениеЗаполнено(Ключ) Тогда
			ПараметрыСообщения.Вставить("key", Ключ);
		КонецЕсли;
		
		ПараметрыСообщения.Вставить("value", Сообщение);
	
		//НовоеСообщение = КоннекторHTTP.ОбъектВJson(ПараметрыСообщения);
		НовоеСообщение = ПараметрыСообщения;
	КонецЕсли;
	
	//СтруктураСообщенияКОтправка = Новый Структура;
	//СтруктураСообщенияКОтправка.Вставить("Сообщение", НовоеСообщение);
	//СтруктураСообщенияКОтправка.Вставить("ДополнительнаяИнформация");
	
	СообщенияПоТопикам = Отправитель.СообщенияПоТопикам;	
	МассивСообщенийТопика = СообщенияПоТопикам.Получить(Топик);
	
	Если МассивСообщенийТопика <> Неопределено Тогда 
		МассивСообщенийТопика.Добавить(НовоеСообщение);
	Иначе
		МассивСообщенийТопика = Новый Массив;
		МассивСообщенийТопика.Добавить(НовоеСообщение);
	КонецЕсли;
	
	СообщенияПоТопикам.Вставить(Топик, МассивСообщенийТопика); 
	
	ВыполнитьЛогированиеДобавлениеСообщения(НовоеСообщение, Отправитель, Ключ, Раздел, Топик);

КонецПроцедуры

#КонецОбласти

#Область Логирование

Процедура ВыполнитьЛогированиеНазначитьПолучателюТопикИРаздел(Подписчик, МетодHTTP, ТекстЗапроса, Ответ)
		
	ОписаниеДействия = НСтр("ru = 'Назначение получателю топика и раздела'");
	ВыполнитьЛогированиеУниверсальное(Подписчик, МетодHTTP, ТекстЗапроса, Ответ, ОписаниеДействия)
	
КонецПроцедуры


Процедура ВыполнитьЛогированиеПерезаписатьСдвиг(Подписчик, МетодHTTP, ТекстЗапроса, Ответ)
	
	ОписаниеДействия = НСтр("ru = 'Перезапись сдвига'");	
	ВыполнитьЛогированиеУниверсальное(Подписчик, МетодHTTP, ТекстЗапроса, Ответ, ОписаниеДействия);

КонецПроцедуры

Процедура ВыполнитьЛогированиеПодтвердитьПолучение(Подписчик, МетодHTTP, ТекстЗапроса, Ответ)
	
	ОписаниеДействия = НСтр("ru = 'Подтверждение получения'");	
	ВыполнитьЛогированиеУниверсальное(Подписчик, МетодHTTP, ТекстЗапроса, Ответ, ОписаниеДействия);

КонецПроцедуры

Процедура ВыполнитьЛогированиеУдалитьПодисчика(Подписчик, МетодHTTP,  Ответ)
	
	ОписаниеДействия = НСтр("ru = 'Удаление подписчика'");
	ВыполнитьЛогированиеУниверсальное(Подписчик, МетодHTTP, "<empty>", Ответ, ОписаниеДействия);

КонецПроцедуры

Процедура ВыполнитьЛогированиеПолучитьСообщения(Подписчик, МетодHTTP, Ответ, МиниммальныйСдвиг, МаксимальныйСдвиг)
	
	ОписаниеДействия = НСтр("ru = 'Получение сообщений'");
	ШаблонТелоЗапроса = НСтр("ru = '<empty> / Получены сообщения сдвигов: с %1 по %2'");      
	ТелоЗапроса = СтрШаблон(ШаблонТелоЗапроса, МиниммальныйСдвиг, МаксимальныйСдвиг);
		
	//Тело ответа не выводится специально, т.к. оно может быть очень большим.
	ВыполнитьЛогированиеУниверсальное(Подписчик, МетодHTTP, ТелоЗапроса, Ответ, ОписаниеДействия, Истина, Ложь);	

КонецПроцедуры

Процедура ВыполнитьЛогированиеЗарегистрироватьПодписчика(Подписчик, МетодHTTP, ТекстЗапроса, Ответ)
	
	ОписаниеДействия = НСтр("ru = 'Зарегистирован подписчик'");	
	ВыполнитьЛогированиеУниверсальное(Подписчик, МетодHTTP, ТекстЗапроса, Ответ, ОписаниеДействия);

КонецПроцедуры

Процедура ВыполнитьЛогированиеПодписаться(Подписчик, МетодHTTP, ТекстЗапроса, Ответ)
	
	ОписаниеДействия = НСтр("ru = 'Подписка'");	
	ВыполнитьЛогированиеУниверсальное(Подписчик, МетодHTTP, ТекстЗапроса, Ответ, ОписаниеДействия);

КонецПроцедуры

Процедура ВыполнитьЛогированиеНовыйПодписчик(Знач ОписаниеСоединения)
		
	Сообщение =  НСтр("ru = 'Создано описание структуры подписчика!'");
	ДополнитьПротоколСоединения(ОписаниеСоединения, Сообщение, Ложь);

КонецПроцедуры

Процедура ВыполнитьЛогированиеНовыйОтправитель(Знач ОписаниеСоединения)
		
	Сообщение =  НСтр("ru = 'Создано описание структуры отправителя!'");
	ДополнитьПротоколСоединения(ОписаниеСоединения, Сообщение, Ложь);

КонецПроцедуры

Процедура ВыполнитьЛогированиеОтправитьСообщения(Отправитель, МетодHTTP, ТекстЗапроса, Ответ)
	
	ОписаниеДействия = НСтр("ru = 'Отправка сообщений'");	
	ВыполнитьЛогированиеУниверсальное(Отправитель, МетодHTTP, ТекстЗапроса, Ответ, ОписаниеДействия);
	
КонецПроцедуры

Процедура ВыполнитьЛогированиеДобавлениеСообщения(НовоеСообщение, Отправитель, Ключ, Раздел, Топик)
		
	ШаблонСообщения = НСтр("ru = 'Добавлено сообщение
	|	Текст сообщения: %1
	|	Топик: %2
	|	Ключ: %3
	|	Раздел: %4'");       
	
	Если ТипЗнч(НовоеСообщение) = Тип("Строка") Тогда
		ТекстСообщения = НовоеСообщение;	
	Иначе 
		ТекстСообщения = КоннекторHTTP.ОбъектВJson(НовоеСообщение);	
	КонецЕсли;
	
	Сообщение = СтрШаблон(ШаблонСообщения, ТекстСообщения, Топик, Ключ, Раздел);
	ДополнитьПротоколСоединения(Отправитель.ОписаниеСоединения, Сообщение, Ложь);

КонецПроцедуры

Процедура ВыполнитьЛогированиеПолучитьСдвиги(Подписчик, МетодHTTP, ТекстЗапроса, Ответ)
	
	ОписаниеДействия = НСтр("ru = 'Получение сдвигов'");	
	ВыполнитьЛогированиеУниверсальное(Подписчик, МетодHTTP, ТекстЗапроса, Ответ, ОписаниеДействия);
	
КонецПроцедуры

Процедура ВыполнитьЛогированиеУниверсальное(Инициатор, МетодHTTP, ТекстЗапроса, Ответ, ОписаниеДействия, ВыводитьТелоЗапроса = Истина, ВыводитьТелоОтвета = Истина)
		
	ТелоОтвет = КоннекторHTTP.КакТекст(Ответ);
	ТелоЗапрос = ТекстЗапроса;
	
	Если Не ВыводитьТелоОтвета Тогда
		ТелоОтвет = НСтр("ru = '<отключен вывод>'");
	КонецЕсли;
		
	ШаблонСообщения = НСтр("ru = '
	|Выполнено: %1
	|	URL: %2  
	|	Метод http: %3	
	|	Тело запроса: %4
	|	Код ответа: %5
	|	Тело ответа %6
	|	Время выполнения: %7'");  
	
	Сообщение = СтрШаблон(
		ШаблонСообщения,
	    ОписаниеДействия,
		Ответ.URL,
		МетодHTTP,
		ТелоЗапрос,
		Ответ.КодСостояния, 
		ТелоОтвет,
		Ответ.ВремяВыполнения);
	
	ДополнитьПротоколСоединения(Инициатор.ОписаниеСоединения, Сообщение, Ложь);

КонецПроцедуры

#КонецОбласти

#Область СлужебныеПроцедурыИФункции

Процедура ВыполнитьКонтрольДопустимыхФорматов(СвойстваСоединения, ВозможныеФорматы)
	
	Формат = СвойстваСоединения.Формат;
	Если ВозможныеФорматы.Найти(Формат) = Неопределено Тогда 
		ЭтоОшибка = Истина;
		ШаблонСообщения = НСтр("ru = 'Формат обмена: %1 не поддерживается данным модулем! '");
		Сообщение = СтрШаблон(ШаблонСообщения, Формат);	
	Иначе
		ЭтоОшибка = Ложь;		
		ШаблонСообщения = НСтр("ru = 'Проверка выполнена успешно. Формат обмена: %1 поддерживается данным модулем. '");
		Сообщение = СтрШаблон(ШаблонСообщения, Формат);
	КонецЕсли;
		
	ДополнитьПротоколСоединения(СвойстваСоединения, Сообщение, ЭтоОшибка)
			
КонецПроцедуры

Процедура ДополнитьПротоколСоединения(СвойстваСоединения, Сообщение, ЭтоОшибка)
	
	РезультатСоединения = СвойстваСоединения.РезультатСоединения;
	РезультатСоединения.ИсторияОпераций.Добавить(Сообщение);
	РезультатСоединения.ИнформацияОбОперации = Сообщение;
	РезультатСоединения.ОшибкаВыполнения = ЭтоОшибка;

КонецПроцедуры

Функция СвойстваПодписчика()
	
	СтруктураПодписчика = Новый Структура;
	СтруктураПодписчика.Вставить("ОписаниеСоединения");
	СтруктураПодписчика.Вставить("ИмяГруппыПодписчиков");
	СтруктураПодписчика.Вставить("ИмяЭкземпляраПодписчика");
	СтруктураПодписчика.Вставить("ИспользоватьАвтоподтверждение");
	СтруктураПодписчика.Вставить("Топики");
	СтруктураПодписчика.Вставить("СдвигТекущий");
	СтруктураПодписчика.Вставить("СдвигПолученныхСообщений");
	СтруктураПодписчика.Вставить("МаксимальныйРазмерОтветаБайт");

	Возврат СтруктураПодписчика;
	
КонецФункции

Функция СвойстваОтправителя()
	
	СообщенияПоТопикам = Новый Соответствие;
	СтруктураОтправитея = Новый Структура;
	СтруктураОтправитея.Вставить("ОписаниеСоединения");
	//СтруктураОтправитея.Вставить("Топик");
	//СтруктураОтправитея.Вставить("Ключ");
	//СтруктураОтправитея.Вставить("Раздел");
	СтруктураОтправитея.Вставить("СообщенияПоТопикам", СообщенияПоТопикам);

	Возврат СтруктураОтправитея;
	
КонецФункции

Процедура ЗаполнитьЗаголовки(Знач СвойстваСоединения, Знач ФорматОбмена)
		
	Если ФорматОбмена = "json" Тогда
		Заголовки = Новый Соответствие;                                                                   
		Заголовки.Вставить("Content-Type", "application/vnd.kafka.json.v2+json");     
		Заголовки.Вставить("Accept", "application/vnd.kafka.json.v2+json");
		ЗаголовкиСтруктура = Новый Структура;
		ЗаголовкиСтруктура.Вставить("Заголовки", Заголовки);

		СвойстваСоединения.Вставить("Заголовки", ЗаголовкиСтруктура);
	КонецЕсли;

КонецПроцедуры

Процедура СгенерироватьИмяПустомуЗначению(Имя)
	
	Если Имя = Неопределено Тогда
		Имя = СокрЛП(Новый УникальныйИдентификатор());
	КонецЕсли;
	
КонецПроцедуры

Функция ЗаголовкиСоединения(Знач ОписаниеСоединения,  ЗаполнитьТипОтправляемогоКонтента = Истина, ЗаполнитьТипПринимаемогоКонтента = Истина)
	
	КопияСтруктуры = Новый Структура;
	Для Каждого ЭлементСтруктуры Из ОписаниеСоединения.Заголовки Цикл
		КопияСтруктуры.Вставить(ЭлементСтруктуры.Ключ, ЭлементСтруктуры.Значение);
	КонецЦикла;
	
	Заголовки = КопияСтруктуры;
	
	Если ЗаполнитьТипПринимаемогоКонтента = Ложь Тогда
		Заголовки.Заголовки.Удалить("Accept");
	КонецЕсли;
	
	Если ЗаполнитьТипОтправляемогоКонтента = Ложь Тогда
		Заголовки.Заголовки.Удалить("Content-Type");
	КонецЕсли;
	
	Возврат Заголовки;

КонецФункции      

#КонецОбласти