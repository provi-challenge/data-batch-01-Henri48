# Provi - Desafio de Data ;)

Você quer entrar na Provi como Engenheiro de Dados? Então você está no lugar certo.

> Se você não quer entrar na área de Engenharia de Dados então você está no lugar errado :). Confira em [carreiras](https://provi.com.br/carreiras) outras vagas.

Leia **todo** esse documento e submita a sua solução.

Esse desafio foi pensado para que você gaste no máximo 3 horas do seu tempo porém fique a vontade para dedicar quanto tempo você quiser.

Boa sorte!

Ah, não querendo ser chato mas já sendo, siga bem atentamente os passos, caso contrário pode ser que a sua submissão não caia em nosso fluxo adequado de feedback.

## Configuração da Máquina

- [Instale o Docker](https://docs.docker.com/get-docker/)
- [Instale o Python](https://www.python.org/downloads/)
    

## Configuração do Desafio

### Repositório

- Use esse repositório como ponto de partida através do GitHub Classroom (mais detalhes no fim do documento).
- Quando você terminar, lembre de fazer `push` em tudo.

### Rodando o banco de dados

Utilizamos PostgreSQL como banco de dados principal, temos um arquivo já configurado utilizando Docker Compose e você pode subir esse banco de dados via Docker com o seguinte comando:

`docker-compose -f docker-compose-pg-only.yml up`


## O que você precisa entregar?

Uma solução que extrai, transforma e armazena as informações [API pública de pokemons (https://pokeapi.co/)](https://pokeapi.co/) que permita análise dos dados.

Bora caçar esses pokemons?

Precisamos informações de *pokemons*, *suas habilidades* e *tipos*:

1. **_Pokemons_**
    1. Nome
    2. Altura
    3. Outras informações que julgar interessantes

2. **_Habilidades_**
    1. Nome
    2. Efeito
    3. Outras informações que julgar interessantes
3. **_Tipos_**
    1. Informações da interação de danos com os demais tipos de pokemons


As informações deverão ser armazenadas de maneira relacional, no formato que julgar melhor. Queremos realizar análises SQL com dados gerados, para isso, precisamos garantir que conseguiremos relacionar as 3 entidades de forma clara e fácil.  


Alguns pontos importantes: 

- A consulta na API deverá ser feita utilizando requisições HTTP.
- Caso o volume de dados seja grande, não há necessidade de fazer a carga completa, apenas garantir que seja possível demonstrar a arquitetura e funcionalidade da solução.
- Não há necessidade alguma de interface visual para o desafio proposto.
- Diagrama com as relações das entidades que você construir. É possível construir um diagrama através do site https://draw.io, mas você pode usar a ferramenta que preferir.
- Os passos devem ser reproduzíveis - desde a extração das informações, criação da base de dados local e o armazenamento dos dados.

Você vai ver no código inicial que muitas partes desse fluxo não existem ou estão incompletos. Isso não é um erro no desafio ;) A sua missão é deixar esse processo o mais completo possível.

Seja criativo!


# Links Úteis

- Docker
    + [Postgres with Docker and Docker compose a step-by-step guide for beginners](https://geshan.com.np/blog/2021/12/docker-postgres/#postgres-with-docker)
    + [Connecting to Postgresql in a docker container from outside](https://stackoverflow.com/questions/37694987/connecting-to-postgresql-in-a-docker-container-from-outside)

## Como resolver o desafio

- Crie um novo repositório utilizando o [GitHub Classroom assignment](https://classroom.github.com/a/RexBdxI8).
- Nosso time vai avaliar sua solução e dar feedback em até 15 dias úteis.
