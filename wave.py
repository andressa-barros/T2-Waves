import pika
import sys
import os

# Verifica se o ID foi passado no terminal
if len(sys.argv) < 2:
    print("Erro: Deve passar o ID do nó. Exemplo: python wave.py 0")
    sys.exit(1)


# 1. VARIÁVEIS DE ESTADO DA ENTIDADE
meu_id = sys.argv[1] 
vizinho_pai = None   
total_mensagens_recebidas = 0
texto_final = meu_id # Inicia com o próprio identificador

# Definindo o nós e seus vizinhos
if meu_id == "0":
    meus_vizinhos = ["1", "3", "4"]
elif meu_id == "1":
    meus_vizinhos = ["0", "2", "3", "4"]
elif meu_id == "2":
    meus_vizinhos = ["1"]
elif meu_id == "3":
    meus_vizinhos = ["0", "1", "4"]
elif meu_id == "4":
    meus_vizinhos = ["0", "1", "3"]
else:
    print("ID inválido! Usa 0, 1, 2, 3 ou 4.")
    sys.exit(1)

eh_iniciador = (meu_id == "0") # O nó 0 é o iniciador


# 2. CONFIGURAÇÃO DO RABBITMQ
# Conecta ao RabbitMQ local
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declara a fila exclusiva para este nó (ex: "fila_0")
minha_fila = f"fila_{meu_id}"
channel.queue_declare(queue=minha_fila)

# 3. FUNÇÃO AUXILIAR DE ENVIO
def enviar_mensagem(destino_id, tipo_msg, conteudo=""):
    """
    Envia a mensagem (R ou I) para a fila do vizinho correspondente.
    O formato da string enviada é: "TIPO|CONTEUDO|REMETENTE"
    """
    msg = f"{tipo_msg}|{conteudo}|{meu_id}"
    channel.basic_publish(exchange='', routing_key=f"fila_{destino_id}", body=msg)
    print(f"[{meu_id}] Enviei {tipo_msg} para o vizinho {destino_id}")

# 4. FUNÇÃO DE RECEBIMENTO (EVENTOS E AÇÕES)
def ao_receber_mensagem(ch, method, properties, body):
    global vizinho_pai, total_mensagens_recebidas, texto_final

    # Desempacota a mensagem recebida
    mensagem = body.decode()
    partes = mensagem.split('|')
    tipo_msg = partes[0]      # "R" (Requisição) ou "I" (Informação)
    info_recebida = partes[1] # Texto dos outros nós (se for I)
    remetente = partes[2]     # Quem enviou a mensagem

    print(f"[{meu_id}] Recebi {tipo_msg} do vizinho {remetente}")
    total_mensagens_recebidas += 1

    # LÓGICA DO INICIADOR
    if eh_iniciador:
        if tipo_msg == "I":
            # Concatena as informações I que recebeu dos vizinhos
            texto_final += info_recebida 
            
        # Quando receber resposta de todos os vizinhos
        if total_mensagens_recebidas == len(meus_vizinhos):
            print(f"\n{'='*40}")
            print(f"[{meu_id}] ALGORITMO TERMINOU!")
            print(f"[{meu_id}] Identificadores recebidos: {texto_final}") # Imprime a lista de identificadores
            print(f"{'='*40}\n")
            os._exit(0) # Força o encerramento do script para o iniciador

    # LÓGICA DA ENTIDADE NÃO-INICIADOR
    else:
        # Quando recebe a requisição R pela primeira vez
        if vizinho_pai is None and tipo_msg == "R":
            vizinho_pai = remetente # Marca a origem como "entrada"
            
            # Repassa R a todas as demais entidades vizinhas, exceto a entrada
            for vizinho in meus_vizinhos:
                if vizinho != vizinho_pai:
                    enviar_mensagem(vizinho, "R")
            
            # Se a entidade for uma borda (recebeu de todos os seus vizinhos, pois só tem 1)
            if total_mensagens_recebidas == len(meus_vizinhos):
                enviar_mensagem(vizinho_pai, "I", texto_final) # Retorna a informação I
                print(f"[{meu_id}] Processo concluído (nó de borda).")

        # Quando recebe mensagens subsequentes (mais R's ou I's)
        else:
            if tipo_msg == "I":
                # Adiciona a sua informação a I
                texto_final += info_recebida 
            
            # Se recebeu R ou I de todos os seus vizinhos
            if total_mensagens_recebidas == len(meus_vizinhos):
                enviar_mensagem(vizinho_pai, "I", texto_final) # Envia de volta para a entrada
                print(f"[{meu_id}] Processo concluído.")

# 5. INÍCIO DA EXECUÇÃO
print(f"[{meu_id}] Nó iniciado. Meus vizinhos são: {meus_vizinhos}")

if eh_iniciador:
    print(f"[{meu_id}] Iniciando a Onda (Wave)...")
    # O iniciador envia a requisição R a todos os seus vizinhos
    for vizinho in meus_vizinhos:
        enviar_mensagem(vizinho, "R")

print(f"[{meu_id}] A aguardar mensagens nas filas do RabbitMQ...")
channel.basic_consume(queue=minha_fila, on_message_callback=ao_receber_mensagem, auto_ack=True)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print(f"\n[{meu_id}] Nó encerrado manualmente.")