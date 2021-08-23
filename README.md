# Arquitetura Orientada a Eventos - Object Storage para o Kafka

Esse é um código de exemplo para utilizar uma arquitetura orientada a eventos que automaticamente lê os arquivos em um bucket de stage, adiciona o nome a uma fila kafka e depois copia os arquivos para o bucket de destino, onde os dados serão processados.
