import socket
import os
import json
import requests

# função para verificar o status do servidor
def verifica_ip(ip, porta):
    try:
        # Cria um objeto socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Define um tempo limite para a conexão (opcional)
        s.settimeout(5)

        # Tenta se conectar ao IP na porta especificada
        s.connect((ip, porta))

        # Fecha a conexão
        s.close()

        return True  # O servidor está em funcionamento
    except (socket.timeout, ConnectionRefusedError):
        return False  # O servidor não está em funcionamento

# IP e porta que você deseja verificar
host = "DATASCIENCE03"
ip = "192.168.1.11"  # Substitua pelo IP que deseja verificar
porta = 3306  # Substitua pela porta que deseja verificar


if __name__ == '__main__':

    if verifica_ip(ip, porta):
        print(f"O servidor {ip}:{porta} está funcionando.")
    else:
        print("SERVER NOT FOUND")