# Modelo Conceitual da Camada Silver

**Entidades e Atributos:**

Buscou-se identificar as principais entidades e seus respectivos atributos para uma modelagem mais efetiva

- Aeroportos

  * Nome do Aeroporto
  * ICAO do Aeroporto
  * Cidade do Aeroporto
  * País do Aeroporto
  * Estado do Aeroporto

- Aeronaves

  * Assentos Business
  * Assentos Economy Plus
  * Assentos Economy
  * Primeiro Voo Aeronave
  * Identificador ICAO
  * Companhia Aérea
  * Fabricante
  * Modelo

- Companhias Aéreas:

  * Nome Empresa
  * Nome ICAO Empresa
  * Infos Institucionais
  * Aliança

- Voos:

  * Número do voo
  * Data de Partida Prevista
  * Data de Partida Real
  * Data Chegada Prevista
  * Data Chegada Real
  * Número de Assentos Ofertados
  * Atraso Minutos Partida
  * Atraso Minutos Chegada

 **Associações e Cardinalidade:**

 - Voos e Aeronaves

    * Cada voo é operado por uma aeronave
    * Uma aeronave pode operar vários voos
    * **Cardinalidade:** N:1 (Voos -> Aeronaves)

- Voos e Companhias Aéreas

    * Cada voo é operado por uma companhia aérea
    * Uma companhia aérea pode estar em vários voos
    * **Cardinalidade:** N:1 (Voos -> Companhia Aérea)

- Voos e Aeroportos

    * Cada voo possui exatamente um aeroporto de origem e destino
    * Em um aeroporto de origem ou destino opera vários voos
    * **Cardinalidade:** N:1 (Voos -> Aeroportos)
