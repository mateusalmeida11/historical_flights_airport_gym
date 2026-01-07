# 1. Descrição do Teste da Camada Bronze

A Camada Bronze pegará o caminho do arquivo gerado pelo processo de ingestão.
A lambda receberá como Payload o nome do Bucket e a Key do arquivo.

A primeira validação é se o Bucket e Key são elementos válidos.
Sendo esses elementos válidos, iniciaremos o processamento desse arquivo.

O processamento final precisará gerar um dataframe com o mesmo número de linhas
do json e com as colunas correspondentes.

Com o processamento executado nós precisamos escrever esses dados em uma camada
Delta no Bucket S3.

Esse é o último processo e caso ele tenha finalizado com sucesso, então podemos
finalizar todo o nosso processo.
