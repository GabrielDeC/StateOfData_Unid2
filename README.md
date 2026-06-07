# Modelagem de Data Warehouse e Automação de Pipeline ETL para Análise de Dados (State of Data 2024/2025)

Este documento faz parte da formação em *Data Analytics* da Digital College e apresenta a documentação completa do projeto de estruturação de um Data Warehouse (DW) e o desenvolvimento de um pipeline de extração, transformação e carga (ETL) utilizando a ferramenta n8n. O objetivo principal do projeto é diagnosticar, mensurar e analisar a **Sobrecarga e os Gargalos Operacionais na Liderança de Dados no Brasil**, tendo como insumo os dados brutos da pesquisa [*State of Data Brazil 2024/2025*](https://www.kaggle.com/datasets/datahackers/state-of-data-brazil-20242025/discussion/587296), realizada pela comunidade Data Hackers em parceria com a Bain & Company.

---

## 1. Racional de Negócio e Definição do Escopo

### 1.1. Contexto do Dataset de Origem
O arquivo original (`Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv`) consiste em uma base de dados analítica de alta dimensionalidade, contendo **5.217 registros** distribuídos em **403 colunas**. Essas colunas guardam respostas a um questionário extenso dividido em 8 seções, cobrindo aspectos desde dados demográficos até o uso avançado de IA Generativa e desafios de gestão de infraestrutura de dados.

### 1.2. Processo de Raciocínio para a Seleção do Problema de Negócio
Durante a fase de exploração conceitual da base, mapeou-se alguns problemas de negócios que poderiam ser analisadas com o dataset. Optei por isolar o tema **Sobrecarga e Gargalos Operacionais na Liderança de Dados**. Os passos analíticos que justificaram essa escolha foram:

1. **Impacto em Cascata (Top-Down):** Times que possuem uma boa operação, retém talentos e têm sucesso técnico estão diretamente condicionados à eficiência de seus gestores.

2. **Filtragem de Ruído Amostral (Redução de Escopo):** Como a maior parte da base é composta por profissionais em níveis operacionais, as respostas de gestão ficavam diluídas. No momento em que restringi o escopo do projeto exclusivamente para líderes, alteramos o foco da análise, consequentemente, tendo densidade de insights em vez de volume de dados genéricos.

3. **Mapeamento de Atributos Booleanos Nativo:** A seção 3 da pesquisa trazia perguntas de múltipla escolha exclusivas para gestores sobre "Maiores Desafios". Neste sentido, o formato possui condição para a criação de indicadores de performance de processos (*KPIs* de governança).

---

## 2. Modelagem do Data Warehouse

Utilizei o PostgreSQL como base para criação do DW. Para garantir a máxima performance em consultas OLAP (analíticas) e relatórios de BI, adotou-se o modelo dimensional **Star Schema (Esquema Estrela)** (Dimensão/Fato) e foi utilizado apenas as linhas de **cada líder/gestor respondente da pesquisa**.

### 2.1. Script DDL de Criação do DW
O script abaixo cria o schema `dw_lideranca`, as tabelas de dimensões, a tabela de fatos e os índices de otimização.

```sql
-- Criação do Schema dedicado para manter a organização analítica
CREATE SCHEMA dw_lideranca;

-- 1. DIMENSÃO: CARGO

CREATE TABLE dim_cargo (
    sk_cargo SERIAL PRIMARY KEY,
    cargo_atual TEXT NOT NULL,
    nivel_senioridade TEXT NOT NULL,
    area_atuacao TEXT,
    CONSTRAINT uniq_cargo_senioridade UNIQUE (cargo_atual, nivel_senioridade)
);

-- 2. DIMENSÃO: LOCALIZAÇÃO

CREATE TABLE dim_localizacao (
    sk_localizacao SERIAL PRIMARY KEY,
    estado_residencia TEXT NOT NULL,
    regiao_brasil TEXT,
    CONSTRAINT uniq_estado UNIQUE (estado_residencia)
);

-- 3. DIMENSÃO: PERFIL DO GESTOR (Demográficos e Carreira)

CREATE TABLE dim_perfil_gestor (
    sk_gestor SERIAL PRIMARY KEY,
    faixa_etaria TEXT,
    genero TEXT,
    nivel_escolaridade TEXT,
    tempo_experiencia_dados TEXT,
    CONSTRAINT uniq_perfil_combinacao UNIQUE (faixa_etaria, genero, nivel_escolaridade, tempo_experiencia_dados)
);

-- 4. DIMENSÃO: AMBIENTE DE TRABALHO E EMPRESA

CREATE TABLE dim_ambiente_trabalho (
    sk_ambiente SERIAL PRIMARY KEY,
    modelo_contratacao TEXT NOT NULL,
    regime_trabalho TEXT,
    tamanho_empresa TEXT NOT NULL,
    CONSTRAINT uniq_ambiente UNIQUE (modelo_contratacao, regime_trabalho, tamanho_empresa)
);

-- 5. TABELA DE FATOS: GARGALOS E SOBRECARGA DA LIDERANÇA

CREATE TABLE fato_desafios_lideranca (
    sk_fato_lideranca BIGSERIAL PRIMARY KEY,

    fk_cargo INT NOT NULL REFERENCES dim_cargo(sk_cargo),
    fk_localizacao INT NOT NULL REFERENCES dim_localizacao(sk_localizacao),
    fk_perfil_gestor INT NOT NULL REFERENCES dim_perfil_gestor(sk_gestor),
    fk_ambiente INT NOT NULL REFERENCES dim_ambiente_trabalho(sk_ambiente),

    tamanho_equipe_gestao TEXT,
    gestor_com_orcamento BOOLEAN,

    gargalo_falta_braco_tecnico BOOLEAN DEFAULT FALSE,
    gargalo_retencao_talentos BOOLEAN DEFAULT FALSE,
    gargalo_provar_valor_area BOOLEAN DEFAULT FALSE,
    gargalo_baixa_maturidade_dados BOOLEAN DEFAULT FALSE,
    gargalo_tecnologias_legadas BOOLEAN DEFAULT FALSE,
    gargalo_burocracia_processos BOOLEAN DEFAULT FALSE,

    nivel_sobrecarga_lideranca INT DEFAULT 0,
    ano_pesquisa INT DEFAULT 2024,
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. ÍNDICES DE PERFORMANCE (Otimização OLAP para Joins Frequentes)

CREATE INDEX idx_fato_lid_cargo ON fato_desafios_lideranca(fk_cargo);
CREATE INDEX idx_fato_lid_localizacao ON fato_desafios_lideranca(fk_localizacao);
CREATE INDEX idx_fato_lid_perfil ON fato_desafios_lideranca(fk_perfil_gestor);
CREATE INDEX idx_fato_lid_ambiente ON fato_desafios_lideranca(fk_ambiente);
```

### 2.2. Explicação Estatutária dos Blocos e Tabelas

* **`dim_cargo`**: Armazena os títulos de liderança (ex: Coordenador, Gerente, Tech Lead) obtidos dinamicamente da base, permitindo segmentar o nível de dor operacional por cargo de comando.
* **`dim_localizacao`**: Guarda os estados e regiões geográficas brasileiras onde o líder reside. Fundamental para entender assimetrias regionais de mercado.
* **`dim_perfil_gestor`**: Reúne as informações demográficas (gênero, idade, escolaridade e tempo de estrada técnica), permitindo análises de diversidade corporativa (ESG) e maturidade individual do gestor.
* **`dim_ambiente_trabalho`**: Concentra o modelo operacional da empresa contratante (remoto, híbrido, tamanho do quadro de funcionários). Essencial para cruzar o nível de stress com a cultura de trabalho da empresa.
* **`fato_desafios_lideranca`**: É o núcleo quantitativo. Ela não contém textos descritivos longos, apenas os IDs de relacionamento e colunas booleanas (`TRUE`/`FALSE`). Isso compacta o banco de dados e acelera agregações matemáticas em bilhões de linhas potenciais.

### 2.3. Racional de Evolução dos Tipos de Dados (Resolução de Erros)

Durante a execução dos testes de carga, a pipeline do n8n quebrou com os erros:
* `value too long for type VARCHAR(50)`
* `value too long for type VARCHAR(100)`

* **Análise e Resolução do Problema**: Como formulários de pesquisas trazem respostas com tamanhos muito imprevisíveis, usar limites estáticos de caracteres acabava quebrando a ingestão de dados. Uma solução que utilizei foi mudar todas as colunas de texto para o tipo TEXT e, com essa troca, o problema foi resolvido.

---

## 3. Pipeline de Ingestão de Dados via N8N

A automação foi desenhada no n8n visando a criação de um pipeline simples e sem falhas.

```text
[Schedule Trigger] ➔ [Google Drive] ➔ [Extract From File] ➔ [Filter] ➔ [Loop] ➔ [Edit Fields] ➔ [Execute Query no PostgreSQL]
```

<img width="1850" height="961" alt="Captura de tela de 2026-06-06 20-32-45" src="https://github.com/user-attachments/assets/63c87dd0-3bd3-4e5e-aff3-f1c5dca83b3e" />

### 3.1. Detalhamento e Justificativa de Cada Nó

* **Schedule / Manual Trigger**: Ponto de entrada do pipeline. Permite a execução sob demanda em desenvolvimento e o agendamento em produção.

* **Google Drive Node (Download a File)**: Realiza a conexão via API com o repositório em nuvem e efetua o download do binário do arquivo CSV original.

* **Extract From File**: É o nó que lê a propriedade vinda do Drive, identifica a codificação CSV e a transforma em objetos estruturados em formato JSON.

* **Filter Node**: Configurado com a condição `{{ $json["2.d_atua_como_gestor"] }} Equal to Sim`. Apenas as linhas dos líderes são mantidas.

* **Loop Node (Controle de Batches)**: Configurado com o tamanho fixo de lote de 100 a 200 itens para evitar cargas muito altas no próximo nó de *Edit Fields*.

* **Edit Fields Node**: Mapeia de forma puramente visual as colunas originais do CSV para chaves no banco de dados. Elimino 380 colunas que não serão utilizadas no fluxo, padroniza nulos utilizando textos (ex: `{{ $json["2.g_nivel"] || 'Não informado' }}`) e realiza a conversão de tipos Boolean e Int.

* **PostgreSQL Node com Execute Query**: Executa a carga consolidada no banco de dados.

### 3.2. Query Consolidada Utilizada no N8N

Para carregar 4 dimensões distintas e amarrar suas chaves autogeradas na tabela de fatos sem criar 5 nós de banco separados no n8n, utilizou-se Common Table Expressions (CTEs) com cláusula WITH e comandos de salvaguarda de duplicidade (ON CONFLICT):

```sql
WITH ins_cargo AS (
    INSERT INTO dw_lideranca.dim_cargo (cargo_atual, nivel_senioridade, area_atuacao)
    VALUES ({{$json.cargo_atual}}, {{$json.nivel_senioridade}}, {{$json.area_atuacao}})
    ON CONFLICT (cargo_atual, nivel_senioridade) DO UPDATE SET area_atuacao = EXCLUDED.area_atuacao
    RETURNING sk_cargo
),
ins_loc AS (
    INSERT INTO dw_lideranca.dim_localizacao (estado_residencia, regiao_brasil)
    VALUES ({{$json.estado_residencia}}', 'Não informado)
    ON CONFLICT (estado_residencia) DO UPDATE SET estado_residencia = EXCLUDED.estado_residencia
    RETURNING sk_localizacao
),
ins_perfil AS (
    INSERT INTO dw_lideranca.dim_perfil_gestor (faixa_etaria, genero, nivel_escolaridade, tempo_experiencia_dados)
    VALUES ({{$json.faixa_etaria}}, {{$json.genero}}, {{$json.nivel_escolaridade}}, {{$json.tempo_experiencia_dados}})
    ON CONFLICT (faixa_etaria, genero, nivel_escolaridade, tempo_experiencia_dados) DO UPDATE SET genero = EXCLUDED.genero
    RETURNING sk_gestor
),
ins_amb AS (
    INSERT INTO dw_lideranca.dim_ambiente_trabalho (modelo_contratacao, regime_trabalho, tamanho_empresa)
    VALUES ({{$json.modelo_contratacao}}, {{$json.modelo_contratacao}}, {{$json.tamanho_empresa}})
    ON CONFLICT (modelo_contratacao, regime_trabalho, tamanho_empresa) DO UPDATE SET tamanho_empresa = EXCLUDED.tamanho_empresa
    RETURNING sk_ambiente
)
INSERT INTO dw_lideranca.fato_desafios_lideranca (
    fk_cargo, fk_localizacao, fk_perfil_gestor, fk_ambiente,
    tamanho_equipe_gestao, gestor_com_orcamento,
    gargalo_falta_braco_tecnico, gargalo_baixa_maturidade_dados,
    gargalo_provar_valor_area, gargalo_tecnologias_legadas,
    gargalo_retencao_talentos, gargalo_burocracia_processos,
    nivel_sobrecarga_lideranca
)
SELECT
    (SELECT sk_cargo FROM ins_cargo),
    (SELECT sk_localizacao FROM ins_loc),
    (SELECT sk_gestor FROM ins_perfil),
    (SELECT sk_ambiente FROM ins_amb),
    {{$json.tamanho_equipe_gestao}}, {{$json.gestor_com_orcamento}},
    {{$json.gargalo_falta_braco_tecnico}}, {{$json.gargalo_baixa_maturidade_dados}},
    {{$json.gargalo_provar_valor_area}}, {{$json.gargalo_tecnologias_legadas}},
    {{$json.gargalo_retencao_talentos}}, {{$json.gargalo_burocracia_processos}},
    {{$json.nivel_sobrecarga_lideranca}};
```

* **Vantagem**: Esta transação é executada de forma única no banco de dados, garantindo que se qualquer inserção falhar, a operação inteira para, mantendo o DW sem precisar ser reescrito.

---

## 4. Queries Analíticas e Geração de Insights

As queries a seguir foram desenvolvidas utilizando técnicas avançadas de agregação do PostgreSQL (filtros inline com a cláusula FILTER), permitindo extrair diagnósticos cirúrgicos para a tomada de decisão da diretoria ou publicação em dashboards.

### Query 1: O "Mapa de Dores" Geral da Liderança
* **Objetivo**: Descobrir qual o percentual exato de líderes afetados por cada um dos gargalos operacionais monitorados no Brasil.
* **Importância**: Permite que consultorias de gestão e diretores de engenharia saibam exatamente qual o maior problema sistêmico do mercado nacional (ex: se é falta de equipe técnica ou infraestrutura legada de dados).

```sql
SELECT 
    COUNT(sk_fato_lideranca) AS "Total de Líderes Analisados",
    ROUND(COUNT(*) FILTER (WHERE gargalo_falta_braco_tecnico) * 100.0 / COUNT(*), 2) AS "% Falta Braço Técnico",
    ROUND(COUNT(*) FILTER (WHERE gargalo_baixa_maturidade_dados) * 100.0 / COUNT(*), 2) AS "% Baixa Maturidade",
    ROUND(COUNT(*) FILTER (WHERE gargalo_provar_valor_area) * 100.0 / COUNT(*), 2) AS "% Dificuldade Provar Valor",
    ROUND(COUNT(*) FILTER (WHERE gargalo_tecnologias_legadas) * 100.0 / COUNT(*), 2) AS "% Sofrem com Legado",
    ROUND(COUNT(*) FILTER (WHERE gargalo_burocracia_processos) * 100.0 / COUNT(*), 2) AS "% Burocracia",
    ROUND(COUNT(*) FILTER (WHERE gargalo_retencao_talentos) * 100.0 / COUNT(*), 2) AS "% Dificuldade Reter Talentos"
FROM 
    dw_lideranca.fato_desafios_lideranca;
```

### Query 2: Correlação de Autonomia Orçamentária e Gargalos Operacionais
* **Objetivo**: Verificar se conceder autonomia financeira (orçamento próprio) para o líder de dados reduz os gargalos operacionais de equipe e tecnologia ou se apenas aumenta a percepção de burocracia corporativa.
* **Importância**: Ajuda a alta governança das empresas a decidir se descentralizar o orçamento para os gestores de dados traz eficiência em contratações ou se cria atrito de processos burocráticos internos.

```sql
SELECT 
    CASE WHEN f.gestor_com_orcamento THEN 'Com Orçamento Próprio' ELSE 'Sem Orçamento' END AS "Autonomia Orçamentária",
    COUNT(f.sk_fato_lideranca) AS "Total de Líderes",
    ROUND(COUNT(*) FILTER (WHERE f.gargalo_falta_braco_tecnico) * 100.0 / COUNT(*), 1) AS "% Falta Equipe Técnica",
    ROUND(COUNT(*) FILTER (WHERE f.gargalo_tecnologias_legadas) * 100.0 / COUNT(*), 1) AS "% Sofrem com Legado",
    ROUND(COUNT(*) FILTER (WHERE f.gargalo_burocracia_processos) * 100.0 / COUNT(*), 1) AS "% Relatam Burocracia"
FROM 
    dw_lideranca.fato_desafios_lideranca f
GROUP BY 
    f.gestor_com_orcamento
ORDER BY 
    "Total de Líderes" DESC;
```

### Query 3: Stress Corporativo
* **Objetivo**: Cruzar o porte da empresa com o modelo de trabalho para encontrar onde estão as maiores médias de sobrecarga mental e as piores taxas de atrito (retenção de talentos).
* **Importância**: Identifica quais modelos de trabalho e tamanhos de empresas geram lideranças sobrecarregadas, permitindo atuar preventivamente para evitar o burnout de executivos de tecnologia e a consequente debandada de equipes.

```sql
SELECT 
    da.tamanho_empresa AS "Porte da Empresa",
    da.regime_trabalho AS "Regime de Trabalho",
    ROUND(AVG(f.nivel_sobrecarga_lideranca), 2) AS "Média de Sobrecarga",
    ROUND(COUNT(*) FILTER (WHERE f.gargalo_retencao_talentos) * 100.0 / COUNT(*), 1) AS "% Dificuldade em Reter Talentos",
    COUNT(f.sk_fato_lideranca) AS "Total de Casos"
FROM 
    dw_lideranca.fato_desafios_lideranca f
JOIN 
    dw_lideranca.dim_ambiente_trabalho da ON f.fk_ambiente = da.sk_ambiente
WHERE 
    da.tamanho_empresa != 'Não informado' 
    AND da.regime_trabalho != 'Não informado'
GROUP BY 
    da.tamanho_empresa, 
    da.regime_trabalho
ORDER BY 
    "Média de Sobrecarga" DESC
LIMIT 10;
```

### Query 4: Assimetria Regional de Maturidade Analítica no Brasil
* **Objetivo**: Classificar os estados brasileiros com base no percentual de empresas com baixa maturidade de dados relatado pelos seus respectivos gestores residentes.
* **Importância**: Ajuda empresas em expansão a entender em quais estados o mercado analítico é maduro e onde há polos que sofrem severamente com falta de braço técnico especializado, balizando estratégias de contratação remota regionalizada.

```sql
SELECT 
    dl.estado_residencia AS "Estado (UF)",
    COUNT(f.sk_fato_lideranca) AS "Gestores Mapeados",
    ROUND(COUNT(*) FILTER (WHERE f.gargalo_baixa_maturidade_dados) * 100.0 / COUNT(*), 1) AS "% Empresas com Baixa Maturidade",
    ROUND(COUNT(*) FILTER (WHERE f.gargalo_falta_braco_tecnico) * 100.0 / COUNT(*), 1) AS "% Escassez de Profissionais"
FROM 
    dw_lideranca.fato_desafios_lideranca f
JOIN 
    dw_lideranca.dim_localizacao dl ON f.fk_localizacao = dl.sk_localizacao
WHERE 
    dl.estado_residencia != 'Não informado'
GROUP BY 
    dl.estado_residencia
HAVING 
    COUNT(f.sk_fato_lideranca) > 10
ORDER BY 
    "% Empresas com Baixa Maturidade" DESC;
```

### Query 5: Análise de Impacto de Gênero na Carga de Gestão (Indicadores ESG)
* **Objetivo**: Analisar se o gênero autodeclarado do gestor de dados apresenta correlações estatísticas com o nível médio de sobrecarga ou com o tamanho das equipes delegadas a eles.
* **Importância**: Fundamental para auditorias de sustentabilidade corporativa, governança e comitês de igualdade (ESG). Mede empiricamente se as lideranças femininas sofrem cargas de stress assimétricas ou se comandam equipes de tamanhos desproporcionais às lideranças masculinas no ecossistema técnico.

```sql
SELECT 
    dp.genero AS "Gênero Declarado",
    COUNT(f.sk_fato_lideranca) AS "Total de Líderes",
    ROUND(AVG(f.nivel_sobrecarga_lideranca), 2) AS "Índice Médio de Sobrecarga",
    MODE() WITHIN GROUP (ORDER BY f.tamanho_equipe_gestao) AS "Tamanho de Equipe Mais Frequente"
FROM 
    dw_lideranca.fato_desafios_lideranca f
JOIN 
    dw_lideranca.dim_perfil_gestor dp ON f.fk_perfil_gestor = dp.sk_gestor
WHERE 
    dp.genero != 'Não informado'
GROUP BY 
    dp.genero
ORDER BY 
    "Total de Líderes" DESC;
```

---

## 5. Conclusão e Próximos Passos

Os resultados obtidos através das queries SQL podem abrir frentes de ação tanto para melhorias em infraestruturas técnicas legadas quanto para correções culturais e de distribuição de carga de trabalho pelas frentes de recursos humanos das organizações brasileiras.
