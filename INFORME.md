# Informe – TP Coordinacion

## Multiples clientes

El `messagehandler` asigna a cada conexion un id unico autoincremental que viaja en todos los mensajes internos. Sum, Aggregation y Join mantienen su estado por cliente y lo borran al terminar. El gateway entrega cada resultado al cliente correspondiente. Estos clientes se procesan en paralelo por el pipeline.

## Múltiples Sum

Los Sum consumen `input_queue` como cola de trabajo, por lo que el EOF de un cliente lo recibe uno solo.

La primera solucion planteada fue un exchange de control entre los Sum: el que recibia el EOF lo reenvia a los otros sums, y cada uno enviaba sus sumas parciales al recibir el aviso. Se descarto porque los datos y el aviso llegaban por colas distintas, permitiendo una race condition: un Sum podia recibir el aviso de la cola de control y enviar sus registros antes de procesar datos restantes del cliente que ya se encontraban en la cola de input, perdiendose los datos.

La solucion final vuelve a encolar el EOF por la misma `input_queue`, llevando la lista de Sum que ya lo recibieron: cada Sum que no esta en la lista envia sus registros y su EOF a los Aggregation, se agrega a la lista y, si falta aun faltan Sums por procesar, lo vuelve a encolar

## Múltiples Aggregation y Join

Cada Sum envia cada fruta a un unico Aggregation, elegido con un hash de la fruta y reducido a la cantidad de aggregations con la operacion de modulo (`HASH FRUTA % AGGREGATION_AMOUNT`), y luego envia su EOF a todos. Cada Aggregation espera `SUM_AMOUNT` EOF de un cliente, calcula su top parcial y lo envia a Join. Join espera los `AGGREGATION_AMOUNT` tops parciales del cliente, los combina y envia el top final.