@echo off
REM Reinicia impressora e exclui todos os jobs, deve estar elevado
REM volta e meia o windows e minha impressora se estranham, acho que isso acontece com todo mundo, 
REM as vezes só funcionava reinicializando a máquina. Bem, esse script resolve o problema, só que no processo todos os jobs são eliminados.
REM https://www.partitionwizard.com/partitionmagic/restart-print-spooler-windows-10.html
net stop spooler
if '%errorlevel%' == '0' ( goto ContinueScript ) else ( goto errorMessage )
:ContinueScript
del /Q C:\Windows\System32\Spool\Printers\*.*
net start spooler
goto EndOfScript
:errorMessage
echo O script precisa executar com permissão adiminstrativa
pause
:EndOfScript
