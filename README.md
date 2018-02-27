## csc-bdse
Базовый проект для практики по курсу "Программная инженерия больших данных".

###Состав команды
Колесников Антон

### Что было сделано:
* Реализована функциональность Persistent Storage Unit на базе Postgres.
* Старт и остановка контейнера Postgres с помощью клиента docker из контейнера ноды (с помощью монтирования docker.sock).
* Динамическое создание инфраструктуры для работы контейнеров (связки node-storage) с помощью клиента docker: внутренняя сеть, volume для сохранения данных между запусками postgres контейнера.

### Проблемы, не решенные в коде:
* Graceful shutdown Postgres контейнера. 
Необходимо дожидаться сохранения буфера и журналов Postgres, перед остановкой контейнера. Возможная наивная реализация: послать сигнал postgres'у shutdown, проверять через интервал жив ли процесс, когда процесс умрет, остановить контейнер.
* "Утечка" контейнера. Если нода падает, то созданый ей postgres контейнер придется удалять руками. Возможная наивная реализация: реализовать java client/sh script, который будет стартовать после старта контейнера и пинговать ноду. Когда пинг не получен N раз за период T, контейнер самоуничтожается.
* Образ postgres контейнера создается динамически во время старта узла. Это удобно для небольшого приложения в образовательных целях. Кажется было бы более правильным вынести этап построения образа, а в настройках приложения указывать идентификатор образа, с которого требуется создать контейнер.    
  
## Задания
[Подготовка](INSTALL.md)
[Задание 1](TASK1.md)
