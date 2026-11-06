# TaskQueue em Python

Fila de tarefas persistente com pool de workers em threads, escrita em Python puro. As tarefas ficam em SQLite (sobrevivem a reinícios), falhas são repetidas com **backoff exponencial**, tarefas que esgotam as tentativas viram "dead letters" consultáveis, e o encerramento é gracioso: tarefas em andamento terminam, nada novo é iniciado e o que sobrou permanece no banco. Inclui CLI para enfileirar, processar e inspecionar.

## Por que este projeto

Filas de tarefas em segundo plano (Celery, Sidekiq, SQS com workers, Cloud Tasks) são peça central de quase todo backend: envio de e-mails, processamento de arquivos, integrações com terceiros. Entrevistas de nível sênior perguntam sobre semântica de entrega (pelo menos uma vez, no máximo uma vez), idempotência, retentativas com backoff, tarefas "venenosas" e como parar um serviço sem perder trabalho. Este projeto implementa essas decisões num código pequeno o bastante para ser lido de uma vez.

## Recursos

- `register(nome, função)` (também como decorador) e `enqueue(nome, payload, max_retries, delay)`.
- Persistência em SQLite; payloads e resultados em JSON.
- Pool de workers (`start(workers=N)`), com reivindicação atômica de tarefas (`BEGIN IMMEDIATE`), sem execução duplicada.
- Retentativas com backoff exponencial (`base * fator^(tentativa-1)`, com teto e jitter opcional).
- Estados: `pending`, `running`, `done`, `failed`; erro da última falha e número de tentativas guardados.
- Tarefas agendadas (`delay`) e ordem FIFO por horário de execução.
- Recuperação de falhas: tarefas deixadas como `running` por um processo que caiu voltam a `pending` ao reabrir a fila.
- Encerramento gracioso com `shutdown(timeout=...)` e `wait_idle()`.
- `run_once()` para executar uma tarefa na thread atual (útil em testes e em execução por cron).
- CLI: `enqueue`, `work`, `status`, `list`, com handlers de demonstração (`echo`, `sleep`, `flaky`, `fail`).

## Como executar

Requer Python 3.8 ou superior. Dentro da pasta do projeto:

```bash
python3 -m task_queue --db tasks.db enqueue echo '{"mensagem": "ola"}'
python3 -m task_queue --db tasks.db enqueue flaky '{"p": 0.5}' --retries 6
python3 -m task_queue --db tasks.db enqueue fail --retries 1

# processa com 3 workers e sai quando a fila esvaziar (sem --drain roda até Ctrl+C)
python3 -m task_queue --db tasks.db work --workers 3 --drain --backoff-base 0.1

python3 -m task_queue --db tasks.db status
python3 -m task_queue --db tasks.db list --status failed
```

Como biblioteca:

```python
from task_queue import TaskQueue

queue = TaskQueue("tasks.db", backoff_base=1, backoff_cap=60)

@queue.register("enviar_email")
def enviar_email(payload):
    ...  # deve ser idempotente
    return {"enviado_para": payload["para"]}

queue.enqueue("enviar_email", {"para": "ana@example.com"}, max_retries=5)
queue.start(workers=4)
queue.wait_idle()
queue.shutdown()
```

## Como testar

```bash
python3 -m unittest -v
```

Parte dos testes é determinística (usa `run_once()` e um relógio falso, sem threads nem `sleep`): backoff exato de 2, 4, 8 segundos, "ainda não venceu", limite de tentativas, handler inexistente, payload ou resultado não serializável, ordem FIFO e recuperação de tarefas interrompidas. A outra parte usa threads reais: 200 tarefas e 6 workers processadas exatamente uma vez, execução paralela comprovada por uma barreira, retentativa com intervalos medidos e encerramento gracioso (a tarefa em andamento termina, as outras quatro continuam `pending`).

## Decisões de projeto

- **Semântica pelo menos uma vez (at-least-once).** Se o processo cair durante uma tarefa, ela é reexecutada. É a garantia realista sem protocolos de confirmação em duas fases; por isso os handlers devem ser idempotentes.
- **Reivindicação atômica.** `BEGIN IMMEDIATE`, seleção da tarefa vencida mais antiga e `UPDATE` para `running` ocorrem numa transação, de modo que dois workers (ou dois processos) nunca pegam a mesma tarefa. Um índice `(status, run_at, id)` mantém a busca barata.
- **Uma conexão SQLite protegida por lock.** Simples e segura entre threads; as operações no banco são curtas. O código dos handlers roda fora do lock, então o paralelismo real vem deles (e, em CPython, é útil para tarefas limitadas por E/S por causa do GIL).
- **Backoff exponencial com teto e jitter.** O crescimento evita sobrecarregar um serviço que já está falhando; o teto limita a espera; o jitter (desligado por padrão para testes determinísticos) evita retentativas sincronizadas. `max_retries=N` significa N retentativas além da primeira execução.
- **Dead letters ficam na tabela.** Tarefas `failed` guardam o último erro e podem ser inspecionadas com `list --status failed`; não há descarte automático.
- **Polling com `Condition`.** Workers dormem até um `enqueue` ou até o `poll_interval`; é simples e robusto, ao custo de uma pequena latência para tarefas agendadas e de consultas periódicas ao banco.
- **Encerramento gracioso.** `shutdown` sinaliza a parada, acorda os workers e aguarda as tarefas em andamento (com timeout opcional). As pendentes não são perdidas: continuam no banco para a próxima execução.
- **Custos:** `enqueue` e a reivindicação são O(log n) pelo índice; a memória usada não depende do tamanho da fila.

## Estrutura de pastas

```
TaskQueue/
├── task_queue/
│   ├── __init__.py
│   ├── __main__.py
│   ├── queue.py        # TaskQueue, persistência, workers, shutdown
│   ├── backoff.py      # política de retentativa
│   ├── handlers.py     # handlers de demonstração para a CLI
│   └── cli.py
├── tests/
│   ├── test_backoff.py
│   ├── test_queue.py
│   └── test_cli.py
├── LICENSE
└── README.md
```

## Limitações conhecidas

- Não há timeout por tarefa: uma thread presa num handler não pode ser interrompida (threads de Python não são canceláveis); o `shutdown(timeout=...)` apenas informa que ela ainda está viva.
- Handlers precisam estar registrados em cada processo que processa a fila; a CLI só conhece os handlers de demonstração.
- Sem prioridades, filas nomeadas, dependências entre tarefas nem limite de concorrência por tipo.
- O SQLite limita a escrita simultânea; para alto volume ou várias máquinas, seria preciso um banco ou broker dedicado.
- Tarefas duplicadas não são detectadas (sem chave de idempotência no `enqueue`).
- Com vários processos no mesmo arquivo, uma tarefa `running` de um processo vivo seria reenfileirada por outro que abra a fila; use um processo de workers por banco.

## Licença

MIT. Veja o arquivo [LICENSE](LICENSE).
