-- Variable pour stocker les commandes de suppression
DECLARE @dropCommands NVARCHAR(MAX);

-- Initialiser la variable
SET @dropCommands = '';

-- Générer les commandes DROP TABLE
SELECT @dropCommands += 'DROP TABLE [' + SCHEMA_NAME(schema_id) + '].[' + name + ']; '
FROM sys.tables
WHERE schema_id = SCHEMA_ID('config');

-- Vérifier les commandes générées
PRINT @dropCommands;

-- Exécuter les commandes
EXEC sp_executesql @dropCommands;


IF SCHEMA_ID('config') IS NULL
	EXECUTE('CREATE SCHEMA config')

IF OBJECT_ID('config.initTable', 'U') IS NULL 
CREATE TABLE config.initTable (
	tableName NVARCHAR(50)
	,cbMarq INT PRIMARY KEY IDENTITY(1,1)
)

IF OBJECT_ID('config.ListArticle', 'U') IS NULL
CREATE TABLE config.ListArticle (
    AR_Ref NVARCHAR(50)
    ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.listFJournaux', 'U') IS NULL
CREATE TABLE config.listFJournaux (
    JO_Num NVARCHAR(50)
    ,JO_Type INT
    ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListCondition', 'U') IS NULL 
CREATE TABLE config.ListCondition (
	CO_No INT
	,AR_Ref NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListArticleRessource', 'U') IS NULL
CREATE TABLE config.ListArticleRessource (
	RP_Code NVARCHAR(50)
	,AR_Ref NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListFamCompta', 'U') IS NULL
CREATE TABLE config.ListFamCompta (
	FA_CodeFamille NVARCHAR(50)
	,FCP_Type INT
	,FCP_Champ INT
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListArtStock', 'U') IS NULL
CREATE TABLE config.ListArtStock (
	AR_Ref NVARCHAR(50)
	,DE_No INT
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListArtCompta', 'U') IS NULL
CREATE TABLE config.ListArtCompta (
	AR_Ref NVARCHAR(50)
	,ACP_Type INT
	,ACP_Champ INT
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListArtClient', 'U') IS NULL
CREATE TABLE config.ListArtClient (
	AR_Ref NVARCHAR(50)
	,AC_Categorie INT
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListArtFourniss', 'U') IS NULL
CREATE TABLE config.ListArtFourniss (
	AR_Ref NVARCHAR(50)
	,CT_Num NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListRessourceProd', 'U') IS NULL
CREATE TABLE config.ListRessourceProd (
	RP_Code NVARCHAR(50)
	,RP_Type INT
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListCollaborateur', 'U') IS NULL
CREATE TABLE config.ListCollaborateur (
	CO_No INT
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListComptet', 'U') IS NULL
CREATE TABLE config.ListComptet (
	CT_Num NVARCHAR(50)
	,CT_Type INT
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListDocEntete', 'U') IS NULL
CREATE TABLE config.ListDocEntete (
	DO_Domaine INT
	,DO_Type INT 
	,DO_Piece NVARCHAR(50)
	,DataBaseSource NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListEcritureC', 'U') IS NULL
CREATE TABLE config.ListEcritureC (
	EC_No INT
	,DataBaseSource NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListEcritureA', 'U') IS NULL
CREATE TABLE config.ListEcritureA (
	EC_No INT
	,DataBaseSource NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListDocLigne', 'U') IS NULL
CREATE TABLE config.ListDocLigne (
	DO_Domaine INT
	,DO_Type INT 
	,DO_Piece NVARCHAR(50)
	,DataBaseSource NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListDocRegl', 'U') IS NULL
CREATE TABLE config.ListDocRegl (
	DO_Domaine INT
	,DO_Type INT 
	,DO_Piece NVARCHAR(50)
	,DR_No INT
	,DataBaseSource NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListReglEch', 'U') IS NULL
CREATE TABLE config.ListReglEch (
	DO_Domaine INT
	,DO_Type INT 
	,DO_Piece NVARCHAR(50)
	,DR_No INT
	,RG_No INT
	,DataBaseSource NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListReglement', 'U') IS NULL
CREATE TABLE config.ListReglement (
	RG_No INT
	,DataBaseSource NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListLivraison', 'U') IS NULL
CREATE TABLE config.ListLivraison (
	LI_No INT
	,CT_Num NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListFamille', 'U') IS NULL
CREATE TABLE config.ListFamille (
	FA_CodeFamille NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListCatTarif', 'U') IS NULL
CREATE TABLE config.ListCatTarif (
	cbIndice INT
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListDepotEmpl', 'U') IS NULL
CREATE TABLE config.ListDepotEmpl (
	DP_No INT
    ,DataBaseSource NVARCHAR(50)
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListDepot', 'U') IS NULL
CREATE TABLE config.ListDepot (
                                  DE_No INT
    ,DataBaseSource NVARCHAR(50)
    ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListCaisse', 'U') IS NULL
CREATE TABLE config.ListCaisse (
     CA_No INT
    ,DataBaseSource NVARCHAR(50)
    ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.ListTaxe', 'U') IS NULL
CREATE TABLE config.ListTaxe (
      TA_Code VARCHAR(50)
      ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.listJMouv', 'U') IS NULL
CREATE TABLE config.listJMouv (
	JO_Num NVARCHAR(50)
	,JM_Date smalldatetime
	,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.SelectTable', 'U') IS NULL
CREATE TABLE [config].[SelectTable](
	[tableName] [nvarchar](50) NULL,
    [lastSynchro] [smalldatetime] NULL,
    [isLoaded] [int] NULL,
	[cbMarq] INT PRIMARY KEY IDENTITY(1,1)
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.listFEnumCond', 'U') IS NULL
CREATE TABLE config.listFEnumCond (
                                      EC_Enumere NVARCHAR(50)
    ,EC_Champ INT
    ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

    IF OBJECT_ID('config.ListCompteg', 'U') IS NULL
CREATE TABLE config.ListCompteg (
                                    CG_Num NVARCHAR(50)
    ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

    IF OBJECT_ID('config.listFTarifCond', 'U') IS NULL
CREATE TABLE config.listFTarifCond (
                                         AR_Ref NVARCHAR(50)
    ,CO_No INT
    ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

    IF OBJECT_ID('config.listPConditionnement', 'U') IS NULL
CREATE TABLE config.listPConditionnement (
                                             cbIndice INT
    ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

    IF OBJECT_ID('config.listPUnite', 'U') IS NULL
CREATE TABLE config.listPUnite (
                                             cbIndice INT
    ,cbMarq INT
    ,DateCreation DATETIME DEFAULT GETDATE()
)

IF OBJECT_ID('config.DB_Errors', 'U') IS NULL
CREATE TABLE [config].[DB_Errors]
         (ErrorID        INT IDENTITY(1, 1),
          UserName       VARCHAR(100),
          ErrorNumber    INT,
          ErrorState     INT,
          ErrorSeverity  INT,
          ErrorLine      INT,
          ErrorProcedure VARCHAR(MAX),
          ErrorMessage   VARCHAR(MAX),
		  TableLoad		 VARCHAR(MAX),
          Query			 VARCHAR(MAX),
          ErrorDateTime  DATETIME)

    IF COL_LENGTH('dbo.F_DOCREGL', 'cbMarqSource') IS NULL
ALTER TABLE F_DOCREGL ADD cbMarqSource INT

    IF COL_LENGTH('config.ListCollaborateur', 'DateCreation') IS NULL
ALTER TABLE config.ListCollaborateur ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listComptet', 'DateCreation') IS NULL
ALTER TABLE config.listComptet ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listLivraison', 'DateCreation') IS NULL
ALTER TABLE config.listLivraison ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listFamille', 'DateCreation') IS NULL
ALTER TABLE config.listFamille ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listFamCompta', 'DateCreation') IS NULL
ALTER TABLE config.listFamCompta ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listArticle', 'DateCreation') IS NULL
ALTER TABLE config.listArticle ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listCondition', 'DateCreation') IS NULL
ALTER TABLE config.listCondition ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.ListArticleRessource', 'DateCreation') IS NULL
ALTER TABLE config.ListArticleRessource ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listRessourceProd', 'DateCreation') IS NULL
ALTER TABLE config.listRessourceProd ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listArtCompta', 'DateCreation') IS NULL
ALTER TABLE config.listArtCompta ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listArtClient', 'DateCreation') IS NULL
ALTER TABLE config.listArtClient ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listArtFourniss', 'DateCreation') IS NULL
ALTER TABLE config.listArtFourniss ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listDocEntete', 'DateCreation') IS NULL
ALTER TABLE config.listDocEntete ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listDocRegl', 'DateCreation') IS NULL
ALTER TABLE config.listDocRegl ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listDocLigne', 'DateCreation') IS NULL
ALTER TABLE config.listDocLigne ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listCompteg', 'DateCreation') IS NULL
ALTER TABLE config.listCompteg ADD DateCreation DATETIME DEFAULT GETDATE()
    IF COL_LENGTH('config.listCollaborateur', 'DateCreation') IS NULL
ALTER TABLE config.listCollaborateur ADD DateCreation DATETIME DEFAULT GETDATE()

IF COL_LENGTH('dbo.F_DOCREGL', 'DataBaseSource') IS NULL
ALTER TABLE F_DOCREGL ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('dbo.F_DOCREGL', 'DR_NoSource') IS NULL
ALTER TABLE F_DOCREGL ADD DR_NoSource INT 

IF COL_LENGTH('dbo.F_CREGLEMENT', 'cbMarqSource') IS NULL
ALTER TABLE F_CREGLEMENT ADD cbMarqSource INT 

IF COL_LENGTH('dbo.F_CREGLEMENT', 'DataBaseSource') IS NULL
ALTER TABLE F_CREGLEMENT ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('dbo.F_CREGLEMENT', 'RG_NoSource') IS NULL
ALTER TABLE F_CREGLEMENT ADD RG_NoSource INT 

IF COL_LENGTH('dbo.F_REGLECH', 'cbMarqSource') IS NULL
ALTER TABLE F_REGLECH ADD cbMarqSource INT 

IF COL_LENGTH('dbo.F_REGLECH', 'DataBaseSource') IS NULL
ALTER TABLE F_REGLECH ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('dbo.F_REGLECH', 'DR_NoSource') IS NULL
ALTER TABLE F_REGLECH ADD DR_NoSource INT 

IF COL_LENGTH('dbo.F_REGLECH', 'RG_NoSource') IS NULL
ALTER TABLE F_REGLECH ADD RG_NoSource INT 

IF COL_LENGTH('dbo.F_DOCLIGNE', 'cbMarqSource') IS NULL
ALTER TABLE F_DOCLIGNE ADD cbMarqSource INT          

IF COL_LENGTH('dbo.F_DOCLIGNE', 'DataBaseSource') IS NULL
ALTER TABLE F_DOCLIGNE ADD DataBaseSource NVARCHAR(50)     

IF COL_LENGTH('dbo.F_DOCENTETE', 'DataBaseSource') IS NULL
ALTER TABLE F_DOCENTETE ADD DataBaseSource NVARCHAR(50) 

IF COL_LENGTH('dbo.F_DOCENTETE', 'cbMarqSource') IS NULL
ALTER TABLE F_DOCENTETE ADD cbMarqSource INT

    IF COL_LENGTH('dbo.F_ECRITUREC', 'DataBaseSource') IS NULL
ALTER TABLE F_ECRITUREC ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('dbo.F_ECRITUREC', 'cbMarqSource') IS NULL
ALTER TABLE F_ECRITUREC ADD cbMarqSource INT 

IF COL_LENGTH('dbo.F_ECRITUREC', 'EC_NoSource') IS NULL
ALTER TABLE F_ECRITUREC ADD EC_NoSource INT

IF COL_LENGTH('dbo.F_ECRITUREA', 'DataBaseSource') IS NULL
ALTER TABLE F_ECRITUREA ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('dbo.F_ECRITUREA', 'cbMarqSource') IS NULL
ALTER TABLE F_ECRITUREA ADD cbMarqSource INT

IF COL_LENGTH('dbo.F_DEPOT', 'DataBaseSource') IS NULL
ALTER TABLE F_DEPOT ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('dbo.F_DEPOT', 'DE_NoSource') IS NULL
ALTER TABLE F_DEPOT ADD DE_NoSource INT

IF COL_LENGTH('dbo.F_DEPOT', 'cbMarqSource') IS NULL
ALTER TABLE F_DEPOT ADD cbMarqSource INT

IF COL_LENGTH('dbo.F_CAISSE', 'DataBaseSource') IS NULL
ALTER TABLE F_CAISSE ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('dbo.F_CAISSE', 'CA_NoSource') IS NULL
ALTER TABLE F_CAISSE ADD CA_NoSource INT

    IF COL_LENGTH('dbo.F_CAISSE', 'cbMarqSource') IS NULL
ALTER TABLE F_CAISSE ADD cbMarqSource INT

    IF COL_LENGTH('config.ListDepot', 'DataBaseSource') IS NULL
ALTER TABLE config.ListDepot ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('config.ListDepotEmpl', 'DataBaseSource') IS NULL
ALTER TABLE config.ListDepotEmpl ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('dbo.F_DEPOTEMPL', 'DataBaseSource') IS NULL
ALTER TABLE F_DEPOTEMPL ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('dbo.F_DEPOTEMPL', 'DP_NoSource') IS NULL
ALTER TABLE F_DEPOTEMPL ADD DP_NoSource INT

IF COL_LENGTH('dbo.F_DEPOTEMPL', 'cbMarqSource') IS NULL
ALTER TABLE F_DEPOTEMPL ADD cbMarqSource INT

IF COL_LENGTH('dbo.F_LIVRAISON', 'cbMarqSource') IS NULL
ALTER TABLE F_LIVRAISON ADD cbMarqSource INT

IF COL_LENGTH('dbo.F_LIVRAISON', 'LI_NoSource') IS NULL
ALTER TABLE F_LIVRAISON ADD LI_NoSource INT

IF COL_LENGTH('dbo.F_LIVRAISON', 'DataBaseSource') IS NULL
ALTER TABLE F_LIVRAISON ADD DataBaseSource NVARCHAR(50)

IF COL_LENGTH('config.SelectTable', 'isLoaded') IS NULL
ALTER TABLE [config].[SelectTable] ADD [isLoaded] [int] NULL

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'reports')
BEGIN
    USE [master]
    CREATE LOGIN [reports] WITH PASSWORD=N'reports2012', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
    EXEC master..sp_addsrvrolemember @loginame = N'reports', @rolename = N'sysadmin'
END

