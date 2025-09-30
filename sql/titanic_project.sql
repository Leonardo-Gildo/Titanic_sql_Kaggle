/*******************************************************
      Titânic - Projeto completo em SQL Server | T-SQL | Azure Data Studio
 Arquivo: coleção completa — criação, importação, limpeza,
 validação, análise e submissão (SQL Server - T-SQL)
 Autor: Leonardo Gildo
 Data: 2025
 Todas as queries usadas no curso, com comentários.
 Versão: 1.0
********************************************************/

----------------------------------------------------------------
-- 0) NOTAS GERAIS
--
-- - Execute blocos separadamente no SSMS / ADS.
-- - Ajuste caminhos de arquivo para sua máquina (Windows) ou use
--   docker cp + caminho dentro do container para ambiente Docker.
-- - Use GO entre blocos quando necessário.
----------------------------------------------------------------


/* =======================================================
   1) TABELAS DE STAGING (recebem os CSVs brutos como NVARCHAR)
   ======================================================= */

-- Criação da tabela staging_titanic (usamos NVARCHAR para evitar falhas na importação)
IF OBJECT_ID('dbo.staging_titanic', 'U') IS NOT NULL DROP TABLE dbo.staging_titanic;
GO

CREATE TABLE dbo.staging_titanic (
  PassengerId NVARCHAR(50),
  Survived    NVARCHAR(50),
  Pclass      NVARCHAR(50),
  Name        NVARCHAR(500),
  Sex         NVARCHAR(50),
  Age         NVARCHAR(50),
  SibSp       NVARCHAR(50),
  Parch       NVARCHAR(50),
  Ticket      NVARCHAR(100),
  Fare        NVARCHAR(50),
  Cabin       NVARCHAR(100),
  Embarked    NVARCHAR(10)
);
GO

-- Explicação:
-- Criei a tabela staging com todas as colunas como NVARCHAR.
-- Vantagem: BULK INSERT raramente falha por tipos incompatíveis.
-- Depois convertemos/validamos para tipos corretos em tabelas "clean".

----------------------------------------------------------------
-- Exemplo de BULK INSERT (Windows)
-- Ajuste o caminho abaixo pra sua máquina (ex.: OneDrive local)
----------------------------------------------------------------
/*
BULK INSERT dbo.staging_titanic
FROM 'C:\Users\leona\OneDrive\LEO_ESTUDOS\PROGRAMAÇÃO\UDEMY\SQL SERVER\Project_Titanic\titanic\train.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
*/
-- Explicação:
-- - FIRSTROW = 2 pula o cabeçalho.
-- - ROWTERMINATOR '0x0a' funciona bem para muitos CSVs; se tiver problemas, tente '\n' ou '\r\n'.
-- - Se estiver usando Docker, copie o arquivo para dentro do container e então use o caminho Linux (veja abaixo).

----------------------------------------------------------------
-- Comando para copiar arquivo do Windows para o container Docker
-- (rodar no PowerShell / CMD, NÃO no SQL)
----------------------------------------------------------------
/*
docker cp "C:\Users\leona\OneDrive\LEO_ESTUDOS\PROGRAMAÇÃO\UDEMY\SQL SERVER\Project_Titanic\titanic\train.csv" sql2022:/var/opt/mssql/data/train.csv
*/
-- Explicação:
-- copia train.csv para /var/opt/mssql/data/ dentro do container nomeado sql2022.
-- então use BULK INSERT apontando para '/var/opt/mssql/data/train.csv'.

----------------------------------------------------------------
-- BULK INSERT exemplo (Docker / Linux path)
----------------------------------------------------------------
/*
BULK INSERT dbo.staging_titanic
FROM '/var/opt/mssql/data/train.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
*/
-- Explicação: mesma lógica do Windows, mas caminho é Linux dentro do container.

----------------------------------------------------------------
-- 1.b) staging para test.csv e gender_submission
----------------------------------------------------------------

IF OBJECT_ID('dbo.staging_titanic_test', 'U') IS NOT NULL DROP TABLE dbo.staging_titanic_test;
GO
CREATE TABLE dbo.staging_titanic_test (
    PassengerId NVARCHAR(50),
    Pclass      NVARCHAR(50),
    Name        NVARCHAR(500),
    Sex         NVARCHAR(50),
    Age         NVARCHAR(50),
    SibSp       NVARCHAR(50),
    Parch       NVARCHAR(50),
    Ticket      NVARCHAR(100),
    Fare        NVARCHAR(50),
    Cabin       NVARCHAR(100),
    Embarked    NVARCHAR(10)
);
GO

-- BULK INSERT analogous (ajuste o caminho)
-- BULK INSERT dbo.staging_titanic_test FROM '...test.csv' WITH (...);

IF OBJECT_ID('dbo.staging_gender_submission', 'U') IS NOT NULL DROP TABLE dbo.staging_gender_submission;
GO
CREATE TABLE dbo.staging_gender_submission (
    PassengerId NVARCHAR(50),
    Survived    NVARCHAR(50)
);
GO

-- Explicação:
-- staging_titanic_test não tem coluna Survived (é o CSV de teste).
-- staging_gender_submission é o baseline CSV da Kaggle (PassengerId,Survived).


/* =======================================================
   2) TRATAMENTO E CRIAÇÃO DAS TABELAS "CLEAN" (tipadas)
   ======================================================= */

-- 2.1 Tabela clean do TRAIN (tipos corretos)
IF OBJECT_ID('dbo.titanic_train_clean', 'U') IS NOT NULL DROP TABLE dbo.titanic_train_clean;
GO

CREATE TABLE dbo.titanic_train_clean (
    PassengerId INT PRIMARY KEY,
    Survived    BIT,
    Pclass      TINYINT,
    Name        NVARCHAR(200),
    Sex         NVARCHAR(10),
    Age         FLOAT NULL,
    SibSp       INT,
    Parch       INT,
    Ticket      NVARCHAR(50),
    Fare        DECIMAL(10,2) NULL,
    Cabin       NVARCHAR(50) NULL,
    Embarked    NVARCHAR(5) NULL
);
GO

-- Insert convertendo a partir do staging_titanic (uso de TRY_CAST, NULLIF, COALESCE)
INSERT INTO dbo.titanic_train_clean (
    PassengerId, Survived, Pclass, Name, Sex, Age,
    SibSp, Parch, Ticket, Fare, Cabin, Embarked
)
SELECT
    TRY_CAST(LTRIM(RTRIM(PassengerId)) AS INT),                         -- tenta converter PassengerId
    TRY_CAST(REPLACE(REPLACE(LTRIM(RTRIM(Survived)), CHAR(13), ''), CHAR(10), '') AS BIT),  -- limpa CR/LF e converte
    TRY_CAST(LTRIM(RTRIM(Pclass)) AS TINYINT),
    Name,
    LTRIM(RTRIM(Sex)),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Age)), '') AS FLOAT),                    -- se vazio vira NULL, senão converte
    TRY_CAST(NULLIF(LTRIM(RTRIM(SibSp)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Parch)), '') AS INT),
    NULLIF(LTRIM(RTRIM(Ticket)), ''),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Fare)), '') AS DECIMAL(10,2)),
    NULLIF(LTRIM(RTRIM(Cabin)), ''),
    COALESCE(NULLIF(LTRIM(RTRIM(Embarked)), ''), 'S')                   -- preenche Embarked em branco com 'S'
FROM dbo.staging_titanic;
GO

-- Explicação linha-a-linha:
-- LTRIM/RTRIM: remove espaços à esquerda/direita.
-- REPLACE(..., CHAR(13), ''), REPLACE(..., CHAR(10), ''): remove CR e LF que apareceram no staging.
-- NULLIF(..., ''): transforma string vazia em NULL para depois TRY_CAST resultar em NULL.
-- TRY_CAST: tenta converter; se falhar retorna NULL (seguro para ETL).
-- COALESCE(NULLIF(...), 'S'): se Embarked for vazio ou NULL, substitui por 'S' (porto mais comum).


-- 2.2 Tabela clean do TEST (sem Survived)
IF OBJECT_ID('dbo.titanic_test_clean', 'U') IS NOT NULL DROP TABLE dbo.titanic_test_clean;
GO

CREATE TABLE dbo.titanic_test_clean (
    PassengerId INT PRIMARY KEY,
    Pclass      TINYINT,
    Name        NVARCHAR(200),
    Sex         NVARCHAR(10),
    Age         FLOAT NULL,
    SibSp       INT,
    Parch       INT,
    Ticket      NVARCHAR(50),
    Fare        DECIMAL(10,2) NULL,
    Cabin       NVARCHAR(50) NULL,
    Embarked    NVARCHAR(5) NULL
);
GO

INSERT INTO dbo.titanic_test_clean (
    PassengerId, Pclass, Name, Sex, Age,
    SibSp, Parch, Ticket, Fare, Cabin, Embarked
)
SELECT
    TRY_CAST(LTRIM(RTRIM(PassengerId)) AS INT),
    TRY_CAST(LTRIM(RTRIM(Pclass)) AS TINYINT),
    Name,
    LTRIM(RTRIM(Sex)),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Age)), '') AS FLOAT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(SibSp)), '') AS INT),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Parch)), '') AS INT),
    NULLIF(LTRIM(RTRIM(Ticket)), ''),
    TRY_CAST(NULLIF(LTRIM(RTRIM(Fare)), '') AS DECIMAL(10,2)),
    NULLIF(LTRIM(RTRIM(Cabin)), ''),
    COALESCE(NULLIF(LTRIM(RTRIM(Embarked)), ''), 'S')
FROM dbo.staging_titanic_test;
GO

-- Explicação:
-- Mesmas técnicas do train: limpeza de espaços e CR/LF, TRY_CAST para evitar erro de conversão.


-- 2.3 Tabela clean do GENDER_SUBMISSION (baseline)
IF OBJECT_ID('dbo.titanic_gender_submission_clean', 'U') IS NOT NULL DROP TABLE dbo.titanic_gender_submission_clean;
GO

CREATE TABLE dbo.titanic_gender_submission_clean (
    PassengerId INT PRIMARY KEY,
    Survived    BIT
);
GO

-- Insert com limpeza explícita de CR/LF (char(13) e char(10)) e espaços
INSERT INTO dbo.titanic_gender_submission_clean (PassengerId, Survived)
SELECT
    TRY_CAST(REPLACE(REPLACE(LTRIM(RTRIM(PassengerId)), CHAR(13), ''), CHAR(10), '') AS INT),
    TRY_CAST(REPLACE(REPLACE(LTRIM(RTRIM(Survived)), CHAR(13), ''), CHAR(10), '') AS BIT)
FROM dbo.staging_gender_submission;
GO

-- Explicação:
-- Alguns CSVs trazem caracteres invisíveis (\r \n) dentro do campo. Aqui removemos e convertemos.


/* =======================================================
   3) DETECÇÃO / DIAGNÓSTICO DE SUJEIRA (queries úteis para debug)
   ======================================================= */

-- 3.1 Verificar valores distintos e tamanhos (útil para encontrar CR/LF)
SELECT DISTINCT Survived, LEN(Survived) AS Tamanho, ASCII(RIGHT(Survived,1)) AS UltimoCaractere
FROM dbo.staging_gender_submission;
-- Explicação:
-- Se UltimoCaractere mostrar 13 (CR) ou 10 (LF), sabemos que há caracteres trailing invisíveis.

-- 3.2 Linhas problemáticas com non-digit no PassengerId
SELECT PassengerId
FROM dbo.staging_gender_submission
WHERE TRY_CAST(REPLACE(REPLACE(LTRIM(RTRIM(PassengerId)), CHAR(13), ''), CHAR(10), '') AS INT) IS NULL
AND LTRIM(RTRIM(PassengerId)) <> '';
-- Explicação:
-- Ajuda achar registros que não convertem para inteiro.

-- 3.3 Contagens básicas nas staging (apenas para checagem)
SELECT COUNT(*) AS total_staging_train FROM dbo.staging_titanic;
SELECT COUNT(*) AS total_staging_test FROM dbo.staging_titanic_test;
SELECT COUNT(*) AS total_staging_gender FROM dbo.staging_gender_submission;

----------------------------------------------------------------
-- 4) VALIDAÇÃO DAS TABELAS CLEAN
----------------------------------------------------------------

-- 4.1 Contagem total
SELECT COUNT(*) AS QtdTrain FROM dbo.titanic_train_clean;
SELECT COUNT(*) AS QtdTest FROM dbo.titanic_test_clean;
SELECT COUNT(*) AS QtdGender FROM dbo.titanic_gender_submission_clean;
-- Explicação: valores esperados: train=891, test=418, gender=418

-- 4.2 Inspeção rápida (TOP)
SELECT TOP 10 * FROM dbo.titanic_train_clean;
SELECT TOP 10 * FROM dbo.titanic_test_clean;
SELECT TOP 10 * FROM dbo.titanic_gender_submission_clean;

-- 4.3 Unicidade PassengerId
SELECT PassengerId, COUNT(*) AS qtd
FROM dbo.titanic_train_clean
GROUP BY PassengerId
HAVING COUNT(*) > 1;
-- Explicação: não deve retornar linhas.

-- 4.4 NULLs por coluna (train)
SELECT
    SUM(CASE WHEN Survived IS NULL THEN 1 ELSE 0 END) AS NullSurvived,
    SUM(CASE WHEN Pclass IS NULL THEN 1 ELSE 0 END) AS NullPclass,
    SUM(CASE WHEN Name IS NULL THEN 1 ELSE 0 END) AS NullName,
    SUM(CASE WHEN Sex IS NULL THEN 1 ELSE 0 END) AS NullSex,
    SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) AS NullAge,
    SUM(CASE WHEN SibSp IS NULL THEN 1 ELSE 0 END) AS NullSibSp,
    SUM(CASE WHEN Parch IS NULL THEN 1 ELSE 0 END) AS NullParch,
    SUM(CASE WHEN Ticket IS NULL THEN 1 ELSE 0 END) AS NullTicket,
    SUM(CASE WHEN Fare IS NULL THEN 1 ELSE 0 END) AS NullFare,
    SUM(CASE WHEN Cabin IS NULL THEN 1 ELSE 0 END) AS NullCabin,
    SUM(CASE WHEN Embarked IS NULL THEN 1 ELSE 0 END) AS NullEmbarked
FROM dbo.titanic_train_clean;
-- Explicação: onde há falta de dados (NULLs) para priorizar tratamento.

----------------------------------------------------------------
-- 5) CONSULTAS EXPLORATÓRIAS / EDA (usadas durante nossos exercícios)
----------------------------------------------------------------

-- 5.1 Contagem total de passageiros
SELECT COUNT(*) AS total_passageiros FROM dbo.titanic_train_clean;
-- Explicação: validação simples (esperado 891).

-- 5.2 Quantos sobreviveram vs não sobreviveram
SELECT Survived, COUNT(*) AS qtd
FROM dbo.titanic_train_clean
GROUP BY Survived;
-- Explicação: mostra 0 vs 1.

-- 5.3 Taxa de sobrevivência (%)
SELECT 
    CAST(SUM(CASE WHEN Survived = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 
    AS taxa_sobrevivencia_percent
FROM dbo.titanic_train_clean;
-- Explicação: converte contagem de sobreviventes para porcentagem.

-- 5.4 Sobrevivência por gênero
SELECT Sex, Survived, COUNT(*) AS qtd
FROM dbo.titanic_train_clean
GROUP BY Sex, Survived
ORDER BY Sex, Survived;
-- Explicação: cruza sexo x sobrevivência.

-- 5.5 Idade média dos sobreviventes vs não sobreviventes
SELECT Survived, AVG(Age) AS idade_media
FROM dbo.titanic_train_clean
GROUP BY Survived;
-- Explicação: Age já está como FLOAT no clean; AVG ignora NULLs.

-- 5.6 Distribuição por classe do navio
SELECT Pclass, COUNT(*) AS qtd_passageiros
FROM dbo.titanic_train_clean
GROUP BY Pclass
ORDER BY Pclass;
-- Explicação: quantos por 1ª/2ª/3ª classe

-- 5.7 Sobrevivência por classe
SELECT Pclass, Survived, COUNT(*) AS qtd
FROM dbo.titanic_train_clean
GROUP BY Pclass, Survived
ORDER BY Pclass, Survived;
-- Explicação: cruza classe x sobrevivência.

-- 5.8 Some estatísticas de idade / tarifa
SELECT AVG(Age) AS media_idade, MIN(Age) AS min_idade, MAX(Age) AS max_idade
FROM dbo.titanic_train_clean;

SELECT AVG(Fare) AS media_tarifa, MIN(Fare) AS min_fare, MAX(Fare) AS max_fare
FROM dbo.titanic_train_clean;

----------------------------------------------------------------
-- 6) EXERCÍCIOS PRÁTICOS (resumo das queries que usamos para treinar)
----------------------------------------------------------------
-- Nível 1
-- 1. Liste todos os passageiros do sexo feminino.
SELECT * FROM dbo.titanic_train_clean WHERE Sex = 'female';

-- 2. Liste os 10 primeiros passageiros ordenados pela tarifa (Fare) em ordem decrescente.
SELECT TOP 10 * FROM dbo.titanic_train_clean ORDER BY Fare DESC;

-- 3. Mostre Name, Age e Sex dos passageiros que sobreviveram (Survived = 1).
SELECT Name, Age, Sex FROM dbo.titanic_train_clean WHERE Survived = 1;

-- 4. Liste os passageiros com mais de 60 anos.
SELECT * FROM dbo.titanic_train_clean WHERE Age > 60;

-- 5. Quantos passageiros tinham exatamente 30 anos?
SELECT COUNT(*) AS qtd_30anos FROM dbo.titanic_train_clean WHERE Age = 30;

-- Nível 2 (agregações)
-- 6. Conte quantos passageiros existem em cada classe (Pclass).
SELECT Pclass, COUNT(*) AS qtd FROM dbo.titanic_train_clean GROUP BY Pclass;

-- 7. Calcule a idade média dos passageiros por sexo.
SELECT Sex, AVG(Age) AS media_idade FROM dbo.titanic_train_clean GROUP BY Sex;

-- 8. Descubra a tarifa máxima e mínima paga.
SELECT MAX(Fare) AS max_fare, MIN(Fare) AS min_fare FROM dbo.titanic_train_clean;

-- 9. Conte quantos passageiros tinham SibSp > 2.
SELECT COUNT(*) AS qtd_SibSp_maior_2 FROM dbo.titanic_train_clean WHERE SibSp > 2;

-- 10. Liste quantos passageiros tinham idade desconhecida (Age IS NULL).
SELECT COUNT(*) AS qtd_age_null FROM dbo.titanic_train_clean WHERE Age IS NULL;

-- Nível 3 (CASE, etc.)
-- 11. Criar FaixaEtaria
SELECT Name, Age,
  CASE
    WHEN Age < 12 THEN 'Criança'
    WHEN Age BETWEEN 12 AND 59 THEN 'Adulto'
    ELSE 'Idoso'
  END AS FaixaEtaria
FROM dbo.titanic_train_clean;

-- 12. Mulheres na 1ª classe
SELECT * FROM dbo.titanic_train_clean WHERE Sex = 'female' AND Pclass = 1;

-- 13. Tarifa média de quem sobreviveu vs não sobreviveu
SELECT Survived, AVG(Fare) AS media_fare FROM dbo.titanic_train_clean GROUP BY Survived;

-- 14. Quantos por Embarked
SELECT Embarked, COUNT(*) AS qtd FROM dbo.titanic_train_clean GROUP BY Embarked;

-- 15. Taxa de sobrevivência por sexo (%)
SELECT Sex,
       100.0 * SUM(CASE WHEN Survived = 1 THEN 1 ELSE 0 END) / COUNT(*) AS TaxaSobrevivenciaPct
FROM dbo.titanic_train_clean
GROUP BY Sex;

----------------------------------------------------------------
-- 7) FEATURE ENGINEERING EXEMPLOS (SQL)
-- 7.1 FamilySize
----------------------------------------------------------------
-- Adiciona coluna derivada FamilySize (temporário, SELECT)
SELECT PassengerId, Name, SibSp, Parch, (SibSp + Parch + 1) AS FamilySize
FROM dbo.titanic_train_clean;

-- 7.2 Extrair Title a partir de Name (Mr/Mrs/Miss/Other) - exemplo com PARSENAME/STRING functions
SELECT
    PassengerId,
    Name,
    CASE
      WHEN Name LIKE '%Mrs.%'  OR Name LIKE '%, Mrs.%'  THEN 'Mrs'
      WHEN Name LIKE '%Mr.%'   OR Name LIKE '%, Mr.%'   THEN 'Mr'
      WHEN Name LIKE '%Miss.%' OR Name LIKE '%, Miss.%' THEN 'Miss'
      WHEN Name LIKE '%Master.%' OR Name LIKE '%, Master.%' THEN 'Master'
      ELSE 'Other'
    END AS Title
FROM dbo.titanic_train_clean;
-- Explicação:
-- Extração simples por pattern matching; em produção usar parsing regex no Python para maior robustez.

----------------------------------------------------------------
-- 8) CRIAÇÃO DA TABELA DE SUBMISSÃO E POPULAÇÃO (baseline:
--     mulheres = 1, homens = 0)
----------------------------------------------------------------

IF OBJECT_ID('dbo.titanic_submission', 'U') IS NOT NULL DROP TABLE dbo.titanic_submission;
GO
CREATE TABLE dbo.titanic_submission (
    PassengerId INT,
    Survived INT
);
GO

INSERT INTO dbo.titanic_submission (PassengerId, Survived)
SELECT 
    PassengerId,
    CASE WHEN Sex = 'female' THEN 1 ELSE 0 END AS Survived
FROM dbo.titanic_test_clean;
GO

-- Validação
SELECT COUNT(*) AS qtd_submission FROM dbo.titanic_submission;
SELECT TOP 10 * FROM dbo.titanic_submission;

----------------------------------------------------------------
-- 9) EXPORTAÇÃO: exemplos para gerar CSV
-- 9.1 Exportar pela grade do ADS/SSMS (GUI) => "Export / Save as CSV"
-- 9.2 Export via sqlcmd (linha de comando) - exemplo:
--     (rode no PowerShell, ajuste -S server e -P senha/token)
----------------------------------------------------------------
/*
sqlcmd -S localhost -U sa -P "SUA_SENHA" -d sql_training -Q "SET NOCOUNT ON; SELECT PassengerId, Survived FROM dbo.titanic_submission ORDER BY PassengerId;" -o "C:\Users\leona\Documents\submission.csv" -s"," -W
*/
-- Explicação:
-- -s"," define separador vírgula; -W remove espaços em branco.

-- 9.3 Export via bcp (exemplo):
/*
bcp "SELECT PassengerId, Survived FROM sql_training.dbo.titanic_submission ORDER BY PassengerId" queryout "C:\Users\leona\Documents\submission.csv" -c -t, -S localhost,1433 -U sa -P "SUA_SENHA"
*/
-- Observação: no Windows, ajuste porta/server conforme sua instância.

----------------------------------------------------------------
-- 10) CHECKLIST / QUERIES DE VALIDAÇÃO RÁPIDAS (rodar sempre)
----------------------------------------------------------------
-- Contagens
SELECT COUNT(*) AS total_train FROM dbo.titanic_train_clean;
SELECT COUNT(*) AS total_test  FROM dbo.titanic_test_clean;

-- Estatísticas rápidas
SELECT Survived, COUNT(*) AS qtd FROM dbo.titanic_train_clean GROUP BY Survived;
SELECT Sex, COUNT(*) AS qtd FROM dbo.titanic_train_clean GROUP BY Sex;
SELECT Pclass, COUNT(*) AS qtd FROM dbo.titanic_train_clean GROUP BY Pclass;

----------------------------------------------------------------
-- 11) EXTRAS: exemplos de correções que usamos (CR/LF / spaces)
----------------------------------------------------------------
-- Detectar valores com CR/LF no final:
SELECT PassengerId, Survived
FROM dbo.staging_gender_submission
WHERE LEN(Survived) <> LEN(RTRIM(Survived))
   OR ASCII(RIGHT(Survived,1)) IN (10,13);

-- Corrigir e atualizar staging (se quiser normalizar a origem)
-- (CUIDADO: normalmente damos preferencia a limpar na hora do INSERT → para manter staging cru)
UPDATE dbo.staging_gender_submission
SET Survived = REPLACE(REPLACE(LTRIM(RTRIM(Survived)), CHAR(13), ''), CHAR(10), '')
WHERE Survived IS NOT NULL AND (ASCII(RIGHT(Survived,1)) IN (10,13) OR LEN(Survived) <> LEN(RTRIM(Survived)));

----------------------------------------------------------------
-- 12) EXERCÍCIOS E GABARITOS (resumo rápido)
----------------------------------------------------------------
-- (Os gabaritos já estão nas queries acima. Repetimos aqui os exemplos):
-- Ex.: taxa de sobrevivência por Pclass:
SELECT Pclass, 100.0 * SUM(CASE WHEN Survived = 1 THEN 1 ELSE 0 END)/COUNT(*) AS TaxaPct
FROM dbo.titanic_train_clean
GROUP BY Pclass
ORDER BY Pclass;

-- Ex.: Taxa por sexo:
SELECT Sex, 100.0 * SUM(CASE WHEN Survived = 1 THEN 1 ELSE 0 END)/COUNT(*) AS TaxaPct
FROM dbo.titanic_train_clean
GROUP BY Sex;

----------------------------------------------------------------
-- FIM do arquivo / sugestões
----------------------------------------------------------------

-- Sugestões práticas:
-- - Execute os blocos na ordem: staging -> bulk insert -> clean tables -> inserts -> validações -> análises -> submission.
-- - No Docker, sempre copie CSVs para /var/opt/mssql/data/ e use esse caminho no BULK INSERT.
-- - Preserve a tabela staging como "fonte crua" e refaça inserts para clean quando ajustar regras.
-- - Para próximos passos: crie views com as features (FamilySize, Title), teste regras SQL para subir a pontuação (ex.: mulheres = 1, homens em 1ª classe crianças = 1, etc.)
