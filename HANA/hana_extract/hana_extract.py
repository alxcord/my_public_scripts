#!/usr/bin/python
# -*- coding: utf-8 -*-
"""

Created on Fri Dec	1 12:18:15 2017

@author: Alex Almeida Cordeiro

Script para executar um ou mais consultas SQL num banco de dados HANA 

Ajustes


"""

import pyhdb, optparse, time
import csv, sys, termios
import os.path, getpass, re, json

def getSqlScript(filename):
	sql_content = ""
	with open(filename,'r') as f:
		sql_content = f.read()
	word_list = re.sub("[^\w]", " ",  sql_content.upper()).split()
	# Evita que rodem algum DDL/DML por engano.
	if  "UPDATE" in word_list or "INSERT" in word_list or "DELETE" in word_list:
		print("Comandos DML não suportados, apenas SELECT")
		sql_content = ""
	if  "CREATE" in word_list or "ALTER" in word_list or "TRUNCATE" in word_list or "DROP" in word_list:
		print("Comandos DDL não suportados, apenas SELECT")
		sql_content = ""
	if  "BEGIN" in word_list or "CALL" in word_list:
		print("Blocos (BEGIN/EXEC/CALL não suportados, apenas SELECT")
		sql_content = ""
	return sql_content
   

def run(user, password, host, port, filename, sql_script, clear_data, show_descriptions):
	if os.path.isfile(filename):
		while True:
			confirm = input("O arquivo de saida {} já existe, sobregravar? ".format(filename))
			if confirm.upper() == 'S':
				break
			elif confirm.upper() == 'N':
				return

	with open(filename, 'w') as file_handler:

		csv_writer = csv.writer(file_handler, delimiter=';', quotechar = '"')

	#	 credentials = getPw()

	#	 print ("Usuário = {0}, senha = {1}".format(credentials[0], credentials[1]))
	#	 sys.exit()

		connection = pyhdb.connect(
			host=host,
			port=port,
			user=user,
			password=password,
		)		  

		# no futuro posso incluir suporte a mais de um arquivo
		#  "{0}_{2}.{1}".format(*filename.rsplit('.', 1) + [file_number]) # é uma lista [arquivo, ext] + [numero] = [arq, ext, numero]
		first_script = True
		for sql_command in sql_script.split(";"):
			if sql_command.isspace() or not sql_command: # vazio ou só contem espaços, entre lf, tab
				break # proximo
			
			cursor = connection.cursor()
			print ("-" * 30)
			print (sql_command)
			t_start = time.time()
# incluir tratamento para exceção pyhdb.exceptions.DatabaseError
			cursor.execute(sql_command)
			t_fetch = time.time()
			counter = 0
			desc_fields = []
			#  ('ZVOLFTANT', 5, None, 17, 3, None, 2), ('ZVOLFTAT', 5, None, 17, 3, None, 2), ('ZVOLFTTL', 5, None, 17, 3, None, 2))
			for field_item in cursor.description:
				if clear_data and field_item[0].startswith("/BIC/"):
					desc_fields.append(field_item[0][5:])
				else:
					desc_fields.append(field_item[0])
			if first_script:
				first_script = False
			csv_writer.writerow(desc_fields)
			if show_descriptions:
				desc_fields = []
				for field_item in cursor.description:
					desc_fields.append(field_item[2])
				if first_script:
					first_script = False
				csv_writer.writerow(desc_fields)

			while True:
				res = cursor.fetchmany(1000)
				for line in res:
					csv_writer.writerow(line)
				if len(res) == 0:
					break
				counter += len(res)
				print ("\rProgresso: {:,}		 ".format(counter).replace(",", "."), end = "")
			t_end = time.time() 
			try:
				cursor.close()
			except:
				pass

			print()
			print("tempo exec..: {} segundos".format(t_fetch - t_start))
			print("tempo fetch.: {} segundos".format(t_end - t_fetch))
			print("tempo total.: {} segundos".format(t_end - t_start))

		try:
			connection.close()
		except:
			pass
	


if __name__ == "__main__":

	config_file_name = "config.json"
	default_env = ""
	env_list = ""
	
	try:
		with open(config_file_name, "r") as f:
			env_config = json.load(f)
	except IOError:
		env_config = []
		print ("Arquivo de ambientes (config.json) ausente")

	
	for entry in env_config:
		env_list = env_list + entry["env"] + " "
		if entry["default"] == "S":
			default_env = entry["env"]


	#usage = "Uso: %prog [options] usuário senha arquivo"
	#parser = optparse.OptionParser(usage = usage)
	parser = optparse.OptionParser()

	parser.add_option('-u', '--user',
		action="store", dest="user", default="",
		help="Nome do usuário HANA")

	parser.add_option('-p', '--pass',
		action="store", dest="password", default="",
		help="Nome do usuário HANA")

	parser.add_option('-e', '--env',
		action="store", dest="env", default=default_env,
		help = "Nome do ambiente: " + env_list)

	parser.add_option("-s", "--script", dest="script",
		help = "Arquivo com o Script SQL", metavar="SCRIPT", default="")

	parser.add_option("-f", "--file", dest = "filename",
		help = "Arquivo de saida", metavar = "FILE", default = "")

	parser.add_option("-c", "--clean",
		action="store_true", dest="clean", default=False,
		help="Limpa nomes dos campos (/BIC/)")		

	parser.add_option("-d", "--showdesc",
		action="store_true", dest="descriptions", default=False,
		help="Adiciona uma linha de cabeçalho com as descrições")		

	
	# parser.add_option("-q", "--quiet",
	#				  action="store_false", dest="verbose", default=True,
	#				  help="Suprimir mensagens")

	options, args = parser.parse_args()

	#print ('Query string:', options.query)

#	 print(options)
#	 print(args)
	
	param_ok = True
	cred_user = options.user
	cred_password = options.password
	file_name = options.filename
	sql_file = options.script
	clear_data = options.clean
	show_descriptions = options.descriptions


	if cred_user == "":
		print ("Usuário inválido")
		param_ok = False
	if file_name == "":
		print ("arquivo de saída inválido")
		param_ok = False
	if sql_file == "":
		print ("arquivo SQL inválido")
		param_ok = False	
	if	os.path.isfile(sql_file) == False:
		print ("arquivo SQL inexistente")
		param_ok = False	
	env = options.env.upper()

	for entry in env_config:
		if env == entry["env"]:
			host = entry["host"]
			port = entry["port"]
			break
	if host == "":
		print ("Ambiente inválido")
		param_ok = False


	if cred_password == "" and param_ok:
		print ("senha não informada")
		#cred_password = get_password("Senha de {} :: ".format(cred_user))
		cred_password = getpass.getpass()
		if	cred_password == "": 
			print ("Parametros inválidos")
			param_ok = False
		
	if param_ok:
		#print ("parametros Ok")
		sql_script = getSqlScript (sql_file)
		if sql_script == "":
			print ("SQL inválido")
			param_ok = False


	if param_ok:
		run(cred_user, cred_password, host, port, file_name, sql_script, clear_data, show_descriptions)
	else:
		parser.print_help()
		sys.exit(1)
		

		

		

	

