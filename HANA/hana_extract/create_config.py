#!/usr/bin/python

"""
    Por Ale Almeida Cordeiro
    Cria arquivos de configuração, é feio, eu sei.
"""

import json

config_file_name = "config.json"

def obtem_sim_nao(pergunta):
    resposta = ""
    while resposta not in ["S", "N"]:
        print (pergunta)
        resposta = input().upper()
    return resposta    


def main():
    try:
        with open (config_file_name, "r") as f:
            config = json.load(f)
    except IOError:
        #do nothing
        config = []

    while True:
        print ("Nome do ambiente: ")
        env = input ()
        print ("Host: ")
        address = input()
        print ("Porta: ")
        port = input()
        default = obtem_sim_nao("Ambiente default? (S/N) ")
        config.append({"env": env, "host": address, "port": port, "default": default})

        if obtem_sim_nao("Adicionar outros ambientes? (S/N) ") == "N":
            break

    if obtem_sim_nao("Salvar? ") == "S":
        try:
            with open(config_file_name, "w") as f:
                config = json.dump(config, f)
        except IOError as e:
            print(e)

# executa função principal.
if __name__ == "__main__":
    main()
