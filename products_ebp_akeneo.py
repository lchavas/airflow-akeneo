###
# Author : Lewis Chavas
###
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
#from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import pandas
import requests
import json
import time
import logging

# Default DAG parameters
default_args = {
    'owner': 'Lewis Chavas',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

# Instantiate the DAG
dag = DAG(
    'products_ebp_akeneo',
    default_args=default_args,
    description='Sync EBP articles > Akeneo products (active state, stock, price, supplier and more)',
    max_active_runs=1,
    catchup=False,
    schedule_interval="@hourly",
    tags=["pim"],
)

# Function that will be called by PythonOperator 'fetch_data_task'
def fetch_data_and_iterate(prev_execution_date=None):

    #ID of the database connection, for running the query
    CONN_ID = 'ebp_gestion_commerciale'
    
    # Akeneo API APP credentials and endpoints
    CLIENT_ID = 'xxx'
    SECRET = 'xxx'
    USERNAME = 'ebp_gestion_commerciale_0470'
    PASSWORD = 'xxx'
    ROOT_URL = 'https://akeneo.groupe-fjc.com'
    TOKEN_ENDPOINT = ROOT_URL + '/api/oauth/v1/token'
    PRODUCT_MODELS_ENDPOINT = ROOT_URL + '/api/rest/v1/product-models'
    PRODUCTS_ENDPOINT = ROOT_URL + '/api/rest/v1/products'
    MEDIA_FILES_ENDPOINT = ROOT_URL + '/api/rest/v1/media-files'
    
    #Setting up the error log mail recipients list
    # example : error_log_mailing_list = ['myaccount@gmail.com', 'thisguy@gmail.com']
    error_log_mailing_list = ['myaccount@mail.com']
    #Setting up the error list for the confirmation mail
    error_log_error_list = []

    #SQL query to retrieve EBP articles, that will be loaded into Akeneo as product models and products
    sql_query = """SELECT /* TOP(10) */
      /* Checksum Produit */
      /*BINARY_CHECKSUM(CONCAT(Item.Id, ' - ', Supplier.Name, ' - ', ThirdReference.Reference)) as 'Product ID',*/

      /* Id produit */
      Item.Id as 'Id',

      /* Statut d'activation du produit */
      CASE Item.ActiveState
        WHEN 0 THEN 1 /* Produit actif */
        ELSE 0 /* Produit bloqué ou en sommeil */
      END AS 'EtatActivation', 
    
      /* Prix HT */
      Item.SalePriceVatExcluded as 'PrixVenteHT',

      /* Description TVA */
      Vat.Description AS 'DescriptionTVA',

      /* Taux TVA */
      CAST(COALESCE(Vat.Rate,0) as DECIMAL(10,2)) as 'TauxTVA',

      /* Montant TVA */
      Item.VatAmount AS 'MontantTVA',

      /* Prix TTC */
      Item.SalePriceVatIncluded AS 'PrixVenteTTC',

      /* Prix achat */
      Item.PurchasePrice as 'PrixAchat',

      /* Taux charges en % */
      CAST(COALESCE(Item.ChargeRate,0) as DECIMAL(10,2)) as 'TauxCharges',

      /* Montant des charges */
      Item.ChargeAmount as 'MontantCharges',

      /* Prix de revient */
      Item.CostPrice as 'PrixRevient',

      /* Montant Marge Nette */
      Item.InterestAmount as 'MontantMargeNette',

      /* Taux de Marge Nette - ratio (montant marge nette / coût achat) en % */
      CAST(COALESCE(Item.InterestRate,0) as DECIMAL(10,2)) as 'TauxMargeNette',

      /* Taux de marque - ratio (montant marge nette / prix vente) en % */
      CAST(COALESCE(Item.BrandRate,0) as DECIMAL(10,2)) as 'TauxMarque',

      /* Quantité par défaut */
      CAST(COALESCE(Item.DefaultQuantity,0) AS INTEGER) as 'QuantiteParDefaut',

      /* Quantité par conditionnement */
      CAST(COALESCE(Item.NumberOfItemByPackage,0) AS INTEGER) as 'QuantiteParConditionnement',

      /* Description 'sous conditionnement minimum' */
      Item.xx_sous_conditionnement_minimum as 'ConditionnementMinimal',
    
      /* Fournisseur */
      Supplier.Name as 'Fournisseur',
    
      /* Référence fournisseur */
      ThirdReference.Reference as 'ReferenceFournisseur',

      /* Référence fournisseur */
      ThirdReference.ThirdReferenceCaption as 'NomProduitFournisseur',

      /* Code barre */
      Item.BarCode as 'CodeBarre',

      /* Ecotaxe */
      /* Item.EcotaxId as 'Ecotax',*/

      /* Code article racine */
      Item.xx_Code_article_racine as 'CodeArticleRacine',

      /* Nom article racine en Français */
      Item.xx_Libelle_article_racine as 'NomArticleRacineFR',

      /* Nom article racine en Italien */
      Item.xx_Libelle_article_racine_IT as 'NomArticleRacineIT',

      /* Nom article racine en Français */
      Item.xx_Description_article_racine as 'DescriptionArticleRacineFR',

      /* Nom article racine en Italien */
      Item.xx_Description_article_racine_IT as 'DescriptionArticleRacineIT',

      /* Nom en Français */
      Item.Caption as 'NomFR',

      /* Nom en Italien */
      Item.LocalizableCaption_2 as 'NomIT',

      /* Description Longue */
      Item.DesComClear as 'DescriptionFR',

      /* Description Longue en Italien */
      Item.LocalizableDesCom_Clear_2 as 'DescriptionIT',

      /* Famille */
      Item.FamilyId as 'CodeFamille',

      /* Nom famille */
      ItemFamily.Caption as 'NomFamille',

      /* Code sous-famille */
      Item.SubFamilyId as 'CodeSousFamille',
      
      /* Nom sous-famille */
      ItemSubFamily.Caption as 'NomSousFamille',

      /* Notes */
      Item.NotesClear as 'Notes',

      /* Taille, pointure, contenance */
      Item.xx_Taille_Pointure as 'Taille',

      /* Largeur */
      CAST(COALESCE(Item.Width,0) as DECIMAL(10,2)) as 'Largeur',
    
      /* Hauteur */
      CAST(COALESCE(Item.Height,0) as DECIMAL(10,2)) as 'Hauteur',
    
      /* Longueur (ou profondeur) */
      CAST(COALESCE(Item.Length,0) as DECIMAL(10,2)) as 'Longueur',
    
      /* Poids en grammes */
      CAST(COALESCE(Item.Weight,0) as DECIMAL(10,2)) as 'Poids',

      /* Poids net en grammes */
      CAST(COALESCE(Item.NetWeight,0) as DECIMAL(10,2)) as 'PoidsNet',

      /* Contremarque (0 si en stock, 1 si sur commande fournisseur) */
      Item.IsManagedByCounterMark as 'Contremarque',
      
      /* Délai de livraison fournisseur, en jours calendaires */
      /* ajouter 5 jours ouvrés (ou 7 jours calendaires) pour obtenir le lead time en contremarque */
      CAST(COALESCE(SupplierItem.DeliveryDelay,0) AS INTEGER) as 'DelaiLivraisonFournisseur',

      /* Stock réel (tout dépôt confondu, utiliser la table stockitem pour avoir le stock par dépôt) */
      CAST(COALESCE(Item.RealStock,0) as INTEGER) as 'StockReel',

      /* PUMP - Prix Moyen Pondéré pour valeur stock réel */
      Item.Pump as 'PrixMoyenPondereStockReel',

      /* Valeur stock */
      Item.StockValue as 'ValeurStockReel',

      /* Stock virtuel */
      CAST(COALESCE(Item.VirtualStock,0) as INTEGER) as 'StockVirtuel',

      /* PUMP - Prix Moyen Pondéré pour valeur stock virtuel */
      Item.VirtualPump as 'PrixMoyenPondereStockVirtuel',

      /* Valeur stock virtuel */
      Item.VirtualStockValue as 'ValeurStockVirtuel',

      /* Stock réel conforme */
      CAST(COALESCE(Item.xx_Stock_Reel_Conforme,0) AS INTEGER) as 'StockReelConforme',

      /* Quantité Réservée */
      CAST(COALESCE(Item.xx_Qte_reservee,0) AS INTEGER) as 'QuantiteReservee',

      /* Quantité en prépa BL */
      CAST(COALESCE(Item.xx_Qte_en_prepa_BL,0) AS INTEGER) as 'QuantitePrepaBL',

      /* Date de création du produit */
      CAST(Item.sysCreatedDate AS DATE) as 'DateCreationProduit',

      /* Date de modification du produit */
      CAST(Item.sysModifiedDate AS DATE) as 'DateModificationProduit',

      /* Date de modification du prix de vente */
      CAST(Item.SalePriceModifiedDate AS DATE) as 'DateModificationPrix',
    
      /* URL Image Produit */
      Item.xx_UrlImage as 'URLImageProduit',
      
      /* URL Fiche Technique Produit */
      Item.xx_UrlFicheTechnique as 'URLFicheTechniqueProduit'
    
      /* -------------------------------- */
      /*   ADDITIONAL PRESTASHOP FIELDS   */
      /* TO LOOK OUT FOR IN NEXT VERSIONS */
      /* -------------------------------- */
      /* On sale (0/1) */
      /* Colonne I : Discount amount */
      /* Colonne J : Discount percent */
      /* Colonne K : Discount from (yyyy-mm-dd) */
      /* Colonne L : Discount to (yyyy-mm-dd) */
      /* Colonne M : Reference # */
      /* Colonne Q : EAN13 (Pas utilisée dans EBP) */
      /* Colonne R : UPC (Pas utilisée dans EBP) */
      /* Colonne S : MPN */
      /* Colonne AB : Minimal quantity */
      /* Colonne AC : Low Stock level */
      /* Colonne AD : Receive a low stock alert by email */
      /* Colonne AE : Visibility */
      /* Colonne AF : Additional shipping cost */
      /* Colonne AG : Unity */
      /* Colonne AK : Tags (x,y,z...) */
      /* Colonne AO : URL rewritten */
      /* Colonne AY : Feature(Name:Value:Position) */    
      /* Colonne BA : Condition */
      /* Colonne BB : Customizable (0 = No, 1 = Yes) */
      /* Colonne BC : Uploadable files (0 = No, 1 = Yes) */
      /* Colonne BD : Text fields (0 = No, 1 = Yes) */
      /* Colonne BE : Out of stock action */
      /* Colonne BF : Virtual product */
      /* Colonne BG : File URL */
      /* Colonne BH : Number of allowed downloads */
      /* Colonne BI : Expiration date */
      /* Colonne BJ : Number of days */
      /* Colonne BK : ID / Name of shop */
      /* Colonne BL : Advanced stock management */
      /* Colonne BM : Depends On Stock */
      /* Colonne BN : Warehouse */
      /* Colonne BO : Acessories  (x,y,z...) */

    FROM Item
      LEFT JOIN ItemFamily ON Item.FamilyId = ItemFamily.Id
      LEFT JOIN ItemSubFamily ON Item.SubFamilyId = ItemSubFamily.Id
      LEFT JOIN Vat ON Item.VatId = Vat.Id
      /* SupplierItem does not contain the supplier item info. This info is in fact located in the ThirdRefence table further below */
      LEFT JOIN SupplierItem ON Item.Id = SupplierItem.ItemId AND SupplierItem.MainSupplier = 1
      LEFT JOIN Supplier ON SupplierItem.SupplierId = Supplier.Id
      LEFT JOIN ThirdReference ON SupplierItem.ReferenceId = ThirdReference.Id
    /* Uniquement les articles de type "bien", pas les "service" */
    WHERE Item.ItemType = 0
    AND Item.Id NOT IN(' CUTT0037', 'PORT DOUANE',
    'GAN034107000ETI', 'GAN034110000ETI', 
    'GAN0034L00000BAG', 'GAN0034M00000BAG', 'GAN0034S00000BAG', 'GAN0034XL0000BAG', 'GAN0334L00000BAG', 'GAN0334M00000BAG', 'GAN0334S00000BAG', 'GAN0334XL0000BAG',
    'STY0014VERBLE000', 'STY0016JAUBLE000', 'STY0016VERBLE000',
    'COM01062XLBLA999', 'COM01063XLBLA999', 'COM0106XL0BLA999', 'COM0106XL0BLA998', 'COM0106XL0BLAVEL', 'GRE0001P36000000',
    'GRE0001360000000', 'GRE0001490000000', 'GRE0001P36000000', 'GRE0001P37000000', 'GRE0001P38000000', 'GRE0001P39000000', 'GRE0001P40000000', 'GRE0001P41000000', 'GRE0001P42000000', 'GRE0001P43000000', 'GRE0001P44000000', 'GRE0001P45000000', 'GRE0001P46000000', 'GRE0001P47000000', 'GRE0001P48000000', 'GRE0001P49000000',
    'MAN0008000BLE100',
    'MAR0003100BLE000', 'MAR0003100JAU000',
    'PAN0195LXL000000', 'PAN0195S/M000000',
    'DIST0056-1', 'DIST0056-2', 'DIST0056-3', 'DIST0056-4', 'DIST0056-5', 'DIST0056-6', 'DIST0056-7', 'DIST0056-8', 'DIST0056-9', 'DIST0056-10', 'DIST0056-11', 'DIST0056-12',
    'MAN0008-AH0059', 'TABL0053-1-AH0059', 'TABL0053-1-SUB3-AH', 'TABL0053-AH0059',
    'MASQ0080-AH0016', 'MASQ0093-AH0016', 'MASQ0094-AH0016',
    'BOUC00301', 'BOUC00302', 'BOUC00271',
    'SAC0018111NOISL0',
    'BLO0196XL0BLA30G',
    'FLA0002CIT000000', 'FLA0002FRU000000',
    'BLO01962XLBLA999', 'BLO0196XL0BLC999'
    )
    /* On ne prend pas les codes '-9' en compte car ce sont des codes de destockage */
    AND Item.Id NOT LIKE '%-9'
    AND Item.sysModifiedDate > '""" + prev_execution_date.astimezone().strftime("%Y%m%d %H:%M:%S") + """'
    ORDER BY Item.Id
    """
    
    #logging.info('#############################################')
    #logging.info('### prev_execution_date : ' + prev_execution_date.astimezone().strftime("%Y%m%d %H:%M:%S"))
    #logging.info('### SQL QUERY : ' + sql_query)
    #logging.info('#############################################')
    
    # Get the MSSQL Hook to run some sql on it
    mssql_hook = MsSqlHook(mssql_conn_id=CONN_ID)

    # Fetch the result of the SQL query as a Pandas DataFrame
    df = mssql_hook.get_pandas_df(sql_query)
    
    # DEBUG - log the dataframe (you can limit results using SQL TOP instruction first)
    #logging.info(df)
    #logging.info(df.keys())
    
    # DEV - MOOT IN THIS CASE - Standard cleanup operations for the dataframe
    # 1. Removing products with missing values
    #df.dropna(inplace=True)
    # 2. Removing duplicates
    #df.drop_duplicates(inplace=True)
    # 3. Cleanup some columns, for instance here we remove leading and trailing spaces
    #cols = df.columns
    #for col in cols:
    #    if df.dtypes[col]=='object':
    #        df[col] =df[col].apply(lambda x: x.rstrip().lstrip())
    # 4. Post-treatment of some values, for instance here we replace '?' with 'Unknown'
    #df['some_value'] = df['some_value'].apply(lambda x: 'Unknown' if x == '?' else x)    # 4. Post-treatment of some values, for instance here we replace '?' with 'Unknown'
    df = df.fillna("")


    # Reusable function for grabbing an auth token
    def grab_auth_token():
        token_data = {
            'grant_type': 'password',
            'client_id': CLIENT_ID,
            'client_secret': SECRET,
            'username': USERNAME,
            'password': PASSWORD
        }
        response = requests.post(TOKEN_ENDPOINT, data=token_data)
        return response.json()
    
    # This object will contain the auth token information
    # See https://api.akeneo.com/api-reference.html#Authentication
    auth_token = None
    
    # This datetime will allow us to regenerate the token before it expires
    auth_expiry = None
    
    # This object will contain the default headers 
    headers = None
   

    # List of references that need to be forced to have color and size variant
    # even though they are in a family where it is usually not required
    force_colorsize_family = {
    'BOU0001L00BLE000','BOU0001M00BLE000','BOU0001S00BLE000',
    
    'CAR00031.L000000','CAR00032.L000000','CAR00041.L000000',
    'CAR00051.L000000','CAR00062.L000000','CAR0007750000000',
    'CAR00081.L000000','CAR00121.L000000','CAR00131.L000000',
    'CAR00141.L000000','CAR00142.L000000','CAR00152.L000000',
    'CAR00160100ML000','CAR00161.L000000',
    
    'DIS00011.L000000','DIS00012.L000000','DIS00021.L000000',
    'DIS00032.L000000','DIS00041.L000000','DIS00051.L000000',
    'DIS00052.L000000','DIS00061.L000000',

    'ECO0002001L00000','ECO0002015L00000',
    
    'RAC0006000BLA000','RAC0006000BLE000','RAC0006000JAU000',
    'RAC0006000NOI000','RAC0006000ROU000','RAC0006000VER000',
    'RAC0006BLEMOUSSE',
    
    'RAC0001245BLA000', 'RAC0001245BLE000', 'RAC0001245JAU000',
    'RAC0001300NOI000', 'RAC0001245ORA000', 'RAC0001245ROU000',
    'RAC0001245VER000', 'RAC0001300VIO000', 'RAC0001245VIO000',
    
    'LUN0006BIF000000','LUN0006PRO000000','LUN0006UFO000000',
    'LUN0007BIF000000','LUN0007PRO000000','LUN0007UFO000000',
    'LUN0011PRO000000','LUN0012PRO000000','LUN0012UNI000000',
    
    'SAC0003000NOI000','SAC0006030BLA000','SAC0006030NOI000',
    'SAC0007030NOI000','SAC0008111BLA000','SAC0008111BLE000',
    'SAC0008111JAU000','SAC0008111NOI000','SAC0008111ROU000',
    'SAC0008111TRA000','SAC0008111VER000','SAC0009130NOI000',
    'SAC0009130ROU000','SAC0010130NOI000','SAC0011160NOI000',
    'SAC0012160NOI000','SAC0013200NOI000','SAC0014111NOI000',
    'SAC0015050NOI000','SAC0016111ROU000','SAC0017050BLE000',
    'SAC0017050NOI000','SAC0017111NOI000','SAC0017130NOI000',
    'SAC0018111BLA000','SAC0018111BLE000','SAC0018111NOI000',
    'SAC0018111NOISL0','SAC0018111VER000','SAC0018130NOI000',
    'SAC0018130ROU000','SAC0018150NOI000','SAC0019050NOI000',
    'SAC0020130BLE000','SAC0020130TRA000','SAC0021111NOI000',
    'SAC0023160NOI000','SAC0024200NOI000','SAC0026L00NAT000',
    'SAC0027110TRA000','SAC0028110NOI000','SAC0029000TRA000'}

    # List of references that have a special marking, and need to be forced to a special color
    force_color_as_marking = {
        "CAS0025000BLA017": "blanc_logo_sst",
        "CAS0046000BLA002": "blanc_logo_gmpsst",
        "CAS0046000BLA024": "blanc_logo_sstrli",
        "CAS0046000GRI023": "gris_logo_rli",
        "CAS0046000GRI024": "gris_logo_sstrli",
        "CAS0046000JAU009": "jaune_logo_gmp",
        "CAS0047000JAU009": "jaune_logo_gmp_non_coquee",
        "GAN0442100000HER": "unique_logo_herta",
        "GIL00212XLJAU007": "jaune_logo_gmp_dossard_poitrine",
        "GIL00212XLJAU029": "jaune_logo_gmp_dossard",
        "GIL00212XLJAU033": "jaune_logo_maraichers_poitrine",
        "GIL00212XLORA026": "orange_logo_andros_visiteur_dossard",
        "GIL0021L00JAU007": "jaune_logo_gmp_dossard_poitrine",
        "GIL0021M00JAU007": "jaune_logo_gmp_dossard_poitrine",
        "GIL0021XL0JAU007": "jaune_logo_gmp_dossard_poitrine",
        "GIL0021XL0JAU029": "jaune_logo_andros_dossard",
        "GIL0021XL0ORA026": "orange_logo_andros_visiteur_dossard",
        "GIL0021XL0ORA034": "orange_logo_agrana_dossard"
    }

    # Iterating over each product row in the dataframe
    for index, row in df.iterrows():
        
        # Checking if the Auth token is still undefined (first article) or about to expire
        if (auth_token is None) or (datetime.now() >= auth_expiry):
            # The token is not yet defined, or is ready to expire

            logging.info('Grabbing new Auth token')

            # Grabbing new auth
            auth_token = grab_auth_token()

            # Setting up API request headers
            headers = {
                'Authorization': 'Bearer ' + auth_token['access_token'],
                'Content-Type': 'application/json'
            }

            # Setting up the deadline to 5 minutes (300 seconds) before token expiration
            auth_expiry = datetime.now() + timedelta(seconds=(auth_token['expires_in'] - 300))
            
            # DEBUG - Logging expiry datetime
            logging.info('New token grabbed at ' + datetime.now().strftime("%d/%m/%Y, %H:%M:%S"))
            logging.info('New token set to be replaced at ' + auth_expiry.strftime("%d/%m/%Y, %H:%M:%S"))
        else:
            # Token still valid
            logging.info('Current token remains valid')
        
        logging.info('Processing product ' + row['Id'])

        # Sorting out the enabled status
        enabled = True
        if row['EtatActivation'] == 0: enabled = False
        
        # Sorting out the contremarque status
        contremarque = False
        if row['Contremarque'] == 1: contremarque = True

        # Sorting out the product family and subfamily
        famille = "Produit"
        famillevariant = "ProduitCouleur"
        categories = ['CatalogueBleuagro']
        match row['CodeFamille']:
            #ACC	Accessoires
            # ACACC1	ACCESSOIRES
            # ACBOX1	BOX EPI
            # ACCUT1	CUTTER
            # ACDET1	DETECTEUR DE GAZ
            # ACERG1	ERGONOMIE
            case "ACC":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                match row['CodeSousFamille']:
                    case "ACACC1": categories = ['Accessoire']
                    case "ACBOX1": categories = ['Accessoire_Box_EPI']
                    case "ACCUT1": categories = ['Accessoire_Cutter']
                    case "ACDET1": categories = ['Accessoire_Detecteur_Gaz']
                    case "ACERG1": categories = ['Accessoire_Ergonomie']
                    case _: categories = ['Accessoire']
            #ANT	Anti-chute
            # ANACC1	ACCESSOIRES
            # ANHAR1	HARNAIS
            # ANKIT1	KIT
            # ANLON1	LONGE
            case "ANT":
                famille = "Produit"
                famillevariant = "ProduitCouleurTaille"
                match row['CodeSousFamille']:
                    case "ANACC1": categories = ['AntiChute_Accessoire']
                    case "ANHAR1": categories = ['AntiChute_Harnais']
                    case "ANKIT1": categories = ['AntiChute_Kit']
                    case "ANLON1": categories = ['AntiChute_Longe']
                    case _: categories = ['AntiChute']
            #BRO	BROSSERIE
            # BRACC1	ACCESSOIRES
            # BRBAL1	BALAIS
            # BRBRO1	BROSSE/BALAYETTE
            # BRBRO2	BROSSERIE NON AGRO
            # BRPEL1	PELLE
            # BRRAC1	RACLETTE
            case "BRO":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                match row['CodeSousFamille']:
                    case "BRACC1": categories = ['Brosserie_Accessoire']
                    case "BRBAL1": categories = ['Brosserie_Balai']
                    case "BRBRO1": categories = ['Brosserie_Brosse_Balayette']
                    case "BRBRO2": categories = ['Brosserie_Non_Agro']
                    case "BRPEL1": categories = ['Brosserie_Pelle']
                    case "BRRAC1": categories = ['Brosserie_Raclette']
                    case _: categories = ['Brosserie']
            #CHA	CHAUSSANT
            # CHBAS1	CHAUSSURE A LACETS BASSE
            # CHBOT1	BOTTE
            # CHMON1	CHAUSSURE A LACETS MONTANT
            # CHPRO1	CHAUSSURE DE PRODUCTION
            # CHSEM1	SEMELLE
            # CHUPO1	U POWER
            case "CHA":
                famille = "Produit"
                famillevariant = "ProduitCouleurTaille"
                match row['CodeSousFamille']:
                    case "CHBAS1": categories = ['Chaussant_Chaussure_Basse_Lacets']
                    case "CHBOT1": categories = ['Chaussant_Botte']
                    case "CHMON1": categories = ['Chaussant_Chaussure_Montante_Lacets']
                    case "CHPRO1": categories = ['Chaussant_Chaussure_Production']
                    case "CHSEM1": categories = ['Chaussant_Semelle']
                    case "CHUPO1": categories = ['Chaussant_U_Power']
                    case _: categories = ['Chaussant']
            #DET	DETECTABLE
            # DEACC1	ACCESSOIRES
            # DEBRO1	BROSSERIE
            # DECUT1	CUTTER
            # DEMAR1	MARQUEUR
            # DEPHA1	PHARMACIE
            # DESTY1	STYLO
            case "DET":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                match row['CodeSousFamille']:
                    case "DEACC1": categories = ['Detectable_Accessoire']
                    case "DEBRO1": categories = ['Detectable_Brosserie']
                    case "DECUT1": categories = ['Detectable_Cutter']
                    case "DEMAR1": categories = ['Detectable_Marqueur']
                    case "DEPHA1": categories = ['Detectable_Pharmacie']
                    case "DESTY1": categories = ['Detectable_Stylo']
                    case _: categories = ['Detectable']
            #HYG	HYGIENE
            # HYACC1	ACCESSOIRES
            # HYESS1	ESSUYAGE /PAPIER/DISTRIBUTEUR
            # HYESS2	ESSUYAGE NON TISSE
            # HYPRO1	PRODUIT D'ENTRETIEN
            # HYSAC1	SAC POUBELLES
            # HYSAV1	SAVON ET HYGIENE DES MAINS
            #ISSUE - Produits de la famille HY(Hygiène) ayant une variation par contenance : CAR0003 CAR0004 CAR0005 CAR0006 CAR0007 CAR0008 CAR0012 CAR0013 CAR0014 CAR0015 CAR0016 DIS0001 DIS0002 DIS0003 DIS0004 DIS0005 DIS0006 DIST0056
            #ISSUE - Produits de la famille HY(Hygiène) ayant une variation par parfum : FLA0002
            case "HYG":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                match row['CodeSousFamille']:
                    case "HYACC1": categories = ['Hygiene_Accessoire']
                    case "HYESS1": categories = ['Hygiene_Essuyage_Papier_Distributeur']
                    case "HYESS2": categories = ['Hygiene_Essuyage_Non_Tisse']
                    case "HYPRO1": categories = ['Hygiene_Produit_Entretien']
                    case "HYSAC1": categories = ['Hygiene_Sac_Poubelle']
                    case "HYSAV1": categories = ['Hygiene_Savon']
                    case _: categories = ['Hygiene']
            #LOGI	LOGICUB
            case "LOGI":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                categories = ['Logicub']
            #MARQ	MARQUAGE SUR VETEMENTS
            #ISSUE - Produits de la famille MARQ(MARQUAGE SUR VETEMENTS) créés en tant que biens, mais sont en réalité un service
            case "MARQ":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                categories = ['MarquageSurVetement']
            #MAT	MATERIEL
            case "MAT":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                categories = ['Materiel']
            #PA	Protection auditive
            # PA3MH1	3M/HONEYWELL
            # PABOU1	BOUCHONS JETABLE
            # PABOU2	BOUCHONS REUTILISABLE
            # PACAS1	CASQUE ANTI BRUIT
            case "PA":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                match row['CodeSousFamille']:
                    case "PA3MH1": categories = ['ProtectionAuditive_3M_Honeywell']
                    case "PABOU1": categories = ['ProtectionAuditive_Bouchon_Jetable']
                    case "PABOU2": categories = ['ProtectionAuditive_Bouchon_Reutilisable']
                    case "PACAS1": categories = ['ProtectionAuditive_Casque_Antibruit']
                    case _: categories = ['ProtectionAuditive']
            #PHA	PHARMARCIE
            # PHACC1	ACCESSOIRES
            # PHDEF1	DEFIBRILATEUR
            # PHRIN1	RINCE ŒIL
            # PHTRO1	TROUSSE DE SECOURS
            case "PHA":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                match row['CodeSousFamille']:
                    case "PHACC1": categories = ['Pharmacie_Accessoire']
                    case "PHDEF1": categories = ['Pharmacie_Defibrillateur']
                    case "PHRIN1": categories = ['Pharmacie_Lavage_Oculaire']
                    case "PHTRO1": categories = ['Pharmacie_Trousse_Secours']
                    case _: categories = ['Pharmacie']
            #PM	Protection de la main
            # PMCHA1	GANT CHALEUR
            # PMCHI1	GANT CHIMIE
            # PMCOU1	GANT ANTI COUPURE
            # PMFRO1	GANT FROID
            # PMMAI1	GANT MAILLE
            # PMMAN1	GANT DE MANUTENTION
            case "PM":
                famille = "Produit"
                famillevariant = "ProduitCouleurTaille"
                match row['CodeSousFamille']:
                    case "PMCHA1": categories = ['ProtectionMain_Gant_Chaleur']
                    case "PMCHI1": categories = ['ProtectionMain_Gant_Chimie']
                    case "PMCOU1": categories = ['ProtectionMain_Gant_Anticoupure']
                    case "PMFRO1": categories = ['ProtectionMain_Gant_Froid']
                    case "PMMAI1": categories = ['ProtectionMain_Gant_Maille']
                    case "PMMAN1": categories = ['ProtectionMain_Gant_Manutention']
                    case _: categories = ['ProtectionMain']
            #PR	Protection respiratoire
            # PR3MM1	3M
            # PRDEM1	DEMI MASQUE
            # PRFIL1	FILTRES
            # PRMAS1	MASQUE A PARTICULES
            # PRMAS2	MASQUE COMPLET
            # PRUNI1	UNITE DE VENTILATION
            case "PR":
                famille = "Produit"
                famillevariant = "ProduitCouleurTaille"
                match row['CodeSousFamille']:
                    case "PR3MM1": categories = ['ProtectionRespiratoire_3M']
                    case "PRDEM1": categories = ['ProtectionRespiratoire_Demi_Masque']
                    case "PRFIL1": categories = ['ProtectionRespiratoire_Filtre']
                    case "PRMAS1": categories = ['ProtectionRespiratoire_Masque_Particules']
                    case "PRMAS2": categories = ['ProtectionRespiratoire_Masque_Complet']
                    case "PRUNI1": categories = ['ProtectionRespiratoire_Unite_Ventilation']
                    case _: categories = ['ProtectionRespiratoire']
            #PT	Protection de la tête
            # PTCAS1	CASQUE
            # PTCAS2	CASQUE A VISIERE
            # PTCAS3	CASQUETTE ANTI HEURT
            case "PT":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                match row['CodeSousFamille']:
                    case "PTCAS1": categories = ['ProtectionTete_Casque']
                    case "PTCAS2": categories = ['ProtectionTete_Casque_Visiere']
                    case "PTCAS3": categories = ['ProtectionTete_Casquette_Anti_Heurt']
                    case _: categories = ['ProtectionTete']
            #PY	Protection des yeux
            # PYACC1	ACCESSOIRES
            # PYBOL1	BOLLE
            # PYECR1	ECRAN FACIAUX
            # PYLUN1	LUNETTE
            # PYLUN2	LUNETTE MASQUE
            # PYLVC1	LUNETTE DE CORRECTION
            #ISSUE - Produits de la famille PY(Protection des yeux) ayant une variation par fonctionnalité : LUN0006 LUN0007 LUN0011 LUN0012
            case "PY":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                match row['CodeSousFamille']:
                    case "PYACC1": categories = ['ProtectionOeil_Accessoire']
                    case "PYBOL1": categories = ['ProtectionOeil_Bolle']
                    case "PYECR1": categories = ['ProtectionOeil_Ecran_Facial']
                    case "PYLUN1": categories = ['ProtectionOeil_Lunettes']
                    case "PYLUN2": categories = ['ProtectionOeil_Lunettes_Masque']
                    case "PYLVC1": categories = ['ProtectionOeil_Lunettes_Correction']
                    case _: categories = ['ProtectionOeil']
            #USAUNI	USAGE UNIQUE
            # UUBLO1	BLOUSE
            # UUCAC1	CACHE BARBE
            # UUCAG1	CAGOULE
            # UUCHA1	CHARLOTTE
            # UUCOM1	COMBINAISON
            # UUDIS1	DISTRIBUTEUR
            # UUGAN1	GANT NITRILE
            # UUGAN2	GANT LATEX
            # UUGAN3	GANT VINYLE
            # UUGAN4	GANT POLYETHYLENE
            # UUMAN1	MANCHETTE
            # UUMAS1	MASQUE
            # UUSUR1	SURCHAUSSURE
            # UUTAB1	TABLIER
            case "USAUNI":
                famille = "Produit"
                famillevariant = "ProduitCouleurTaille"
                match row['CodeSousFamille']:
                    case "UUBLO1": categories = ['UsageUnique_Blouse']
                    case "UUCAC1": categories = ['UsageUnique_Cache_Barbe']
                    case "UUCAG1": categories = ['UsageUnique_Cagoule']
                    case "UUCHA1": categories = ['UsageUnique_Charlotte']
                    case "UUCOM1": categories = ['UsageUnique_Combinaison']
                    case "UUDIS1": categories = ['UsageUnique_Distributeur']
                    case "UUGAN1": categories = ['UsageUnique_Gant_Nitrile']
                    case "UUGAN2": categories = ['UsageUnique_Gant_Latex']
                    case "UUGAN3": categories = ['UsageUnique_Gant_Vinyle']
                    case "UUGAN4": categories = ['UsageUnique_Gant_Polyethylene']
                    case "UUMAN1": categories = ['UsageUnique_Manchette']
                    case "UUMAS1": categories = ['UsageUnique_Masque']
                    case "UUSUR1": categories = ['UsageUnique_Surchaussure']
                    case "UUTAB1": categories = ['UsageUnique_Tablier']
                    case _: categories = ['UsageUnique']
            #VET	VETEMENTS
            # VEACC1	ACCESSOIRES
            # VEFRO1	FROIDS
            # VEFRO2	FROIDS NEGATIF
            # VEHTV1	HAUTE VISIBILITE
            # VENET1	NETTOYAGE
            case "VET":
                famille = "Produit"
                famillevariant = "ProduitCouleurTaille"
                match row['CodeSousFamille']:
                    case "VEACC1": categories = ['Vetement_Accessoire']
                    case "VEFRO1": categories = ['Vetement_Froid']
                    case "VEFRO2": categories = ['Vetement_Froid_Negatif']
                    case "VEHTV1": categories = ['Vetement_Haute_Visibilite']
                    case "VENET1": categories = ['Vetement_Nettoyage']
                    case _: categories = ['Vetement']
            #VETTECH	Vetements techniques
            case "VETTECH":
                famille = "Produit"
                famillevariant = "ProduitCouleurTaille"
                categories = ['VetementTechnique']
            #VX	Divers et autres
            case "VX":
                famille = "Produit"
                famillevariant = "ProduitCouleur"
                categories = ['Divers']

        #If the product is a substitute model (ending in -1 or -2), we need to create a substitute product model
        code_racine = row['CodeArticleRacine']
        if row['Id'].endswith("-1"): code_racine += '-1'
        if row['Id'].endswith("-2"): code_racine += '-2'
        if row['Id'].endswith("-EU"): code_racine += '-EU'
        if row['Id'].endswith("-FR"): code_racine += '-FR'
        
        ## OLD - # HACK - First, we check if the Id is more than 3 characters longer that the root code
        ## OLD - #   Because if the root code is within 3 characters of the Id, it is:
        ## OLD - #   - either an article with the same Id as root code
        ## OLD - #   - or an article with Id equal to root code with -1 to -12 or -EU appended at the end
        ## OLD - # Anyway, the product will not be a variant
        ## OLD - #if len(row['Id']) <= (len(row['CodeArticleRacine']) + 3): famillevariant = None
        if row['Id'] in {row['CodeArticleRacine'],row['CodeArticleRacine']+"-1",row['CodeArticleRacine']+"-2",row['CodeArticleRacine']+"-EU",row['CodeArticleRacine']+"-FR"}: famillevariant = None
        
        # If root code is the same as product code, the product is not a variant
        # if row['Id'] == row['CodeArticleRacine']: famillevariant = None
        
        # Check if the product is a forced couleur and taille variant
        if row['Id'] in force_colorsize_family: 
            famillevariant = "ProduitCouleurTaille"
            logging.info('Product ' + row['Id'] + " family overridden to 'ProduitCouleurTaille'")
        
        # DEBUG : logging famille famillevariant and categories
        logging.info('Product ' + row['Id'] + ' has category "' + str(categories) + '" and family "' + str(famille) + '" and family variant "' + str(famillevariant) + '"')
 
        # Testing if the product is a variant or a regular product
        if famillevariant is None:
            #The product is a single product
            
            # Collecting current product data
            # https://api.akeneo.com/api-reference.html#Productidentifier
            product_data = {
                "identifier": row['Id'],
                "family": famille,
                "categories": categories,
                "enabled": enabled,
                "values": {
                    "Couleur": [{"locale": None, "scope": None, "data": 'unique'}],
                    "Taille": [{"locale": None, "scope": None, "data": 'unique'}],
                    "Nom": [{"locale": "fr_FR", "scope": "ebp", "data": row['NomFR']},
                    {"locale": "it_IT", "scope": "ebp", "data": row['NomIT']},
                    {"locale": "fr_FR", "scope": "prestashop", "data": row['NomFR']},
                    {"locale": "it_IT", "scope": "prestashop", "data": row['NomIT']}],
                    "Description": [{"locale": "fr_FR", "scope": "ebp", "data": row['DescriptionFR']},
                    {"locale": "it_IT", "scope": "ebp", "data": row['DescriptionIT']},
                    {"locale": "fr_FR", "scope": "prestashop", "data": row['DescriptionFR']},
                    {"locale": "it_IT", "scope": "prestashop", "data": row['DescriptionIT']}],
                    "Publier": [{"locale": None, "scope": "prestashop", "data": enabled}],
                    "DateDernierePublication": [{"locale": None, "scope": "prestashop", "data": None}],
                    "PrixHT": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixVenteHT']),"currency": "EUR"}]}],
                    "PrixTTC": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixVenteTTC']),"currency": "EUR"}]}],
                    "DescriptionTVA": [{"locale": None, "scope": None, "data": row['DescriptionTVA']}],
                    "TauxTVA": [{"locale": None, "scope": None, "data": str(row['TauxTVA'])}],
                    "MontantTVA": [{"locale": None, "scope": None, "data": [{"amount": str(row['MontantTVA']),"currency": "EUR"}]}],
                    "PrixAchat": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixAchat']),"currency": "EUR"}]}],
                    "TauxCharges": [{"locale": None, "scope": None, "data": str(row['TauxCharges'])}],
                    "MontantCharges": [{"locale": None, "scope": None, "data": [{"amount": str(row['MontantCharges']),"currency": "EUR"}]}],
                    "PrixRevient": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixRevient']),"currency": "EUR"}]}],
                    "MontantMargeNette": [{"locale": None, "scope": None, "data": [{"amount": str(row['MontantMargeNette']),"currency": "EUR"}]}],
                    "TauxMargeNette": [{"locale": None, "scope": None, "data": str(row['TauxMargeNette'])}],
                    "TauxMarque": [{"locale": None, "scope": None, "data": str(row['TauxMarque'])}],
                    "QuantiteParDefaut": [{"locale": None, "scope": None, "data": str(row['QuantiteParDefaut'])}],
                    "QuantiteParConditionnement": [{"locale": None, "scope": None, "data": str(row['QuantiteParConditionnement'])}],
                    "ConditionnementMinimal": [{"locale": None, "scope": None, "data": str(row['ConditionnementMinimal'])}],
                    "Fournisseur": [{"locale": None, "scope": None, "data": str(row['Fournisseur'])}],
                    "ReferenceFournisseur": [{"locale": None, "scope": None, "data": str(row['ReferenceFournisseur'])}],
                    "NomProduitFournisseur": [{"locale": None, "scope": None, "data": str(row['NomProduitFournisseur'])}],
                    "CodeBarre": [{"locale": None, "scope": None, "data": str(row['CodeBarre'])}],
                    "DelaiLivraisonFournisseur": [{"locale": None, "scope": None, "data": str(row['DelaiLivraisonFournisseur'])}],
                    "Longueur": [{"locale": None, "scope": None, "data": {"amount": str(row['Longueur']),"unit": "MILLIMETER"}}],
                    "Largeur": [{"locale": None, "scope": None, "data": {"amount": str(row['Largeur']),"unit": "MILLIMETER"}}],
                    "Hauteur": [{"locale": None, "scope": None, "data": {"amount": str(row['Hauteur']),"unit": "MILLIMETER"}}],
                    "Poids": [{"locale": None, "scope": None, "data": {"amount": str(row['Poids']),"unit": "GRAM"}}],
                    "PoidsNet": [{"locale": None, "scope": None, "data": {"amount": str(row['PoidsNet']),"unit": "GRAM"}}],
                    "Contremarque": [{"locale": None, "scope": None, "data": contremarque}],
                    "StockReel": [{"locale": None, "scope": None, "data": str(row['StockReel'])}],
                    "PrixMoyenPondereStockReel": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixMoyenPondereStockReel']),"currency": "EUR"}]}],
                    "ValeurStockReel": [{"locale": None, "scope": None, "data": [{"amount": str(row['ValeurStockReel']),"currency": "EUR"}]}],
                    "StockVirtuel": [{"locale": None, "scope": None, "data": str(row['StockVirtuel'])}],
                    "PrixMoyenPondereStockVirtuel": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixMoyenPondereStockVirtuel']),"currency": "EUR"}]}],
                    "ValeurStockVirtuel": [{"locale": None, "scope": None, "data": [{"amount": str(row['ValeurStockVirtuel']),"currency": "EUR"}]}],
                    "StockReelConforme": [{"locale": None, "scope": None, "data": str(row['StockReelConforme'])}],
                    "QuantiteReservee": [{"locale": None, "scope": None, "data": str(row['QuantiteReservee'])}],
                    "QuantitePrepaBL": [{"locale": None, "scope": None, "data": str(row['QuantitePrepaBL'])}],
                }
            }
            
            # DEV - Dump product data
            # logging.info('Product data : ' + json.dumps(product_data))
            
            # Checking PIM to see if the product exists
            response = requests.get(PRODUCTS_ENDPOINT + '/' + row['Id'], headers=headers)
            
            if response.status_code == 200:
                # Product exists, it's an update
                logging.info('Product ' + row['Id'] + ' found. Updating product data...')
                response = requests.patch(PRODUCTS_ENDPOINT + '/' + row['Id'], headers=headers, json=product_data)
                # Checking response
                if response.status_code == 204:
                    # Code 204 returned after successful update
                    logging.info('Product ' + row['Id'] + ' updated successfully.')
                else:
                    logging.info('[**ERROR**] : ERROR UPDATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
                    error_log_error_list.append('ERROR UPDATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
            else:
                # Product does not exist, it needs to be created
                logging.info('Product ' + row['Id'] + ' not found. Creating product...')
                response = requests.post(PRODUCTS_ENDPOINT, headers=headers, json=product_data)
                # Checking response
                if response.status_code == 201:
                    # Code 201 returned after successful creation
                    logging.info('Product ' + row['Id'] + ' created successfully.')
                else:
                    logging.info('[**ERROR**] : ERROR CREATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
                    error_log_error_list.append('ERROR CREATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
            
        else:
            #The product is a variant product
            
            # Collecting product model data (the root of the variant product)
            # https://api.akeneo.com/api-reference.html#Productmodel
            product_model_data = {
                "code": code_racine,
                "family": famille,
                "family_variant": famillevariant,
                "categories": categories,
                "values": {
                    "Nom": [{"locale": "fr_FR", "scope": "ebp", "data": row['NomArticleRacineFR']},
                    {"locale": "it_IT", "scope": "ebp", "data": row['NomArticleRacineIT']},
                    {"locale": "fr_FR", "scope": "prestashop", "data": row['NomArticleRacineFR']},
                    {"locale": "it_IT", "scope": "prestashop", "data": row['NomArticleRacineIT']}],
                    "Description": [{"locale": "fr_FR", "scope": "ebp", "data": row['DescriptionArticleRacineFR']},
                    {"locale": "it_IT", "scope": "ebp", "data": row['DescriptionArticleRacineIT']},
                    {"locale": "fr_FR", "scope": "prestashop", "data": row['DescriptionArticleRacineFR']},
                    {"locale": "it_IT", "scope": "prestashop", "data": row['DescriptionArticleRacineIT']}]
                }
            }
            
            
            # Checking PIM to see if the product model exists
            response = requests.get(PRODUCT_MODELS_ENDPOINT + '/' + code_racine, headers=headers)
            
            if response.status_code == 200:
                # Product model exists, it's an update
                logging.info('Product model ' + code_racine + ' found. Updating product model data...')
                response = requests.patch(PRODUCT_MODELS_ENDPOINT + '/' + code_racine, headers=headers, json=product_model_data)
                # Checking response
                if response.status_code == 204:
                    # Code 204 returned after successful update
                    logging.info('Product model ' + code_racine + ' updated successfully.')
                else:
                    logging.info('[**ERROR**] : ERROR UPDATING PRODUCT MODEL ' + code_racine + ' : ' + chr(response.status_code) + ' - ' + response.text)
                    error_log_error_list.append('ERROR UPDATING PRODUCT MODEL ' + code_racine + ' : ' + chr(response.status_code) + ' - ' + response.text)
            else:
                # Product model does not exist, it needs to be created
                logging.info('Product model ' + code_racine + ' not found. Creating product model...')
                response = requests.post(PRODUCT_MODELS_ENDPOINT, headers=headers, json=product_model_data)
                # Checking response
                if response.status_code == 201:
                    # Code 201 returned after successful creation
                    logging.info('Product model ' + code_racine + ' created successfully.')
                else:
                    logging.info('[**ERROR**] : ERROR CREATING PRODUCT MODEL ' + code_racine + ' : ' + chr(response.status_code) + ' - ' + response.text)
                    error_log_error_list.append('ERROR CREATING PRODUCT MODEL ' + code_racine + ' : ' + chr(response.status_code) + ' - ' + response.text)

            ### OLD - # Finding out color and size through positional means
            ### OLD - # HACK - First, we check if the Id is more than 3 characters longer that the root code
            ### OLD - #   Because if the root code is within 3 characters of the Id, it is:
            ### OLD - #   - either an article with the same Id as root code
            ### OLD - #   - or an article with Id equal to root code with -1 to -12 or -EU appended at the end
            ### OLD - if len(row['Id']) <= (len(row['CodeArticleRacine']) + 3):
            if row['Id'] == row['CodeArticleRacine']:
                couleur = 'unique'
                taille = 'unique'
            else:
                # HACK - the first characters of the ID is the product model, let's remove it from the Id to have better comparison results. Let's also remove zeroes.
                clean_id = row['Id'].replace(row['CodeArticleRacine'],'')

                #Test if it is a product with markings, for which color needs to be forced
                if row['Id'] in force_color_as_marking:
                    couleur = force_color_as_marking[row['Id']]
                else:
                    #Check if we have some positional color data
                    tentative_color = clean_id[3:6].casefold()
                    tentative_color_match = True
                    match tentative_color:
                        case '000': couleur = 'unique'
                        case 'nat': couleur = 'unique'
                        case 'l00': couleur = 'unique'
                        case '008': couleur = 'unique'
                        case '009': couleur = 'unique'
                        case '00e': couleur = 'unique'
                        case '080': couleur = 'unique'
                        case '0ml': couleur = 'unique'
                        case '700': couleur = 'unique'
                        case 'o00': couleur = 'unique'
                        case 'ble': 
                            if clean_id[6:9].casefold() == 'cla': couleur = 'bleuclair'
                            else: couleur = 'bleu'
                        case 'blb': couleur = 'bleu'
                        case 'bug': couleur = 'bleu'
                        case 'blr': couleur = 'bleu'
                        case 'blc': couleur = 'bleuclair'
                        case 'mou': couleur = 'bleu'
                        case '001': couleur = 'bleu'
                        case '0re': couleur = 'bleu'
                        case '003': couleur = 'blanc'
                        case '005': couleur = 'blanc'
                        case '100': couleur = 'blanc'
                        case 'sub': couleur = 'blanc'
                        case 'ah0': couleur = 'blanc'
                        case '00m': couleur = 'bleumarine'
                        case '0ma': couleur = 'bleumarine'
                        case 'gls': couleur = 'bleumarine'
                        case 'mry': couleur = 'bleumarine'
                        case 'r00': couleur = 'bleumarine'
                        case 'ar0': couleur = 'bleumarine'
                        case 'rou': couleur = 'rouge'
                        case 'ro0': couleur = 'rouge'
                        case 'ver': couleur = 'vert'
                        case '0ve': couleur = 'vert'
                        case 'ven': couleur = 'vert'
                        case 'q01': couleur = 'vert'
                        case 'kak': couleur = 'kaki'
                        case 'bla': couleur = 'blanc'
                        case '00b': couleur = 'blanc'
                        case '0bl': couleur = 'blanc'
                        case 'b0l': couleur = 'blanc'
                        case 'l0b': couleur = 'blanc'
                        case 'noi': couleur = 'noir'
                        case '00n': couleur = 'noir'
                        case 'oi0': couleur = 'noir'
                        case '0ni': couleur = 'noir'
                        case '0no': couleur = 'noir'
                        case 'bei': couleur = 'beige'
                        case 'nte': couleur = 'beige'
                        case 'sab': couleur = 'beige'
                        case 'gri': couleur = 'gris'
                        case 'lgr': couleur = 'gris'
                        case 'car': couleur = 'gris'
                        case 'gre': couleur = 'gris'
                        case 'gno': couleur = 'gris'
                        case 'ant': couleur = 'grisanthracite'
                        case 'grc': couleur = 'grisanthracite'
                        case 'jau': couleur = 'jaune'
                        case 'au0': couleur = 'jaune'
                        case 'mar': couleur = 'marron'
                        case 'mro': couleur = 'marron'
                        case 'ora': couleur = 'orange'
                        case 'ros': couleur = 'rose'
                        case 'rr0': couleur = 'rose'
                        case 'vio': couleur = 'violet'
                        case 'voi': couleur = 'violet'
                        case 'jno': couleur = 'jaune_noir'
                        case 'jas': couleur = 'jaunefluo'
                        case 'jfm': couleur = 'jaunefluo_bleumarine'
                        case 'jal': couleur = 'jaunefluo_bleumarine'
                        case '00j': couleur = 'jaunefluo_bleumarine'
                        case '0jf': couleur = 'jaunefluo_bleumarine'
                        case 'ofm': couleur = 'orangefluo_bleumarine'
                        case 'orm': couleur = 'orangefluo_bleumarine'
                        case 'orl': couleur = 'orangefluo_bleumarine'
                        case 'jfb': couleur = 'jaunefluo_bleu'
                        case 'jab': couleur = 'jaunefluo_bleu'
                        case 'jfv': couleur = 'jaunefluo_vert'
                        case 'jav': couleur = 'jaunefluo_vert'
                        case 'tra': couleur = 'transparent'
                        case 'ass': couleur = 'assortiment'
                        case _: tentative_color_match = False
                        
                    if tentative_color_match: 
                        # Color found through positional means
                        logging.info("Color '" + couleur + "' inferred from positional substring '" + tentative_color + "'")
                    else: 
                        # Could not find a color through positional means, finding out color from caption or other parts of Id
                        logging.info("NO COLOR INFERRED from positional substring '" + tentative_color + "'")
                        # Color is a unique attribute, let's set up a unique mock color then try to guess the color from row['NomFR'] or row['Id']
                        couleur = "zzz_" + row['Id'].translate(str.maketrans(".-/\ ", "_____"))
                        if "bleu" in row['NomFR'].casefold() or "BLE" in clean_id: couleur = "bleu"
                        if "bleu clair" in row['NomFR'].casefold() or "BLECLA" in clean_id: couleur = "bleuclair"
                        if "rouge" in row['NomFR'].casefold() or "ROU" in clean_id: couleur = "rouge"
                        if "vert" in row['NomFR'].casefold() or "VER" in clean_id: couleur = "vert"
                        if "blanc" in row['NomFR'].casefold() or "BLA" in clean_id: couleur = "blanc"
                        if "noir" in row['NomFR'].casefold() or "NOI" in clean_id: couleur = "noir"
                        if "beige" in row['NomFR'].casefold() or "BEI" in clean_id: couleur = "beige"
                        if "gris" in row['NomFR'].casefold() or "GRI" in clean_id: couleur = "gris"
                        if "jaune" in row['NomFR'].casefold() or "JAU" in clean_id: couleur = "jaune"
                        if "marine" in row['NomFR'].casefold() or "MAR" in clean_id or "MRY" in clean_id: couleur = "bleumarine"
                        if "orange" in row['NomFR'].casefold() or "ORA" in clean_id: couleur = "orange"
                        if "rose" in row['NomFR'].casefold() or "ROS" in clean_id: couleur = "rose"
                        if "violet" in row['NomFR'].casefold() or "VIO" in clean_id: couleur = "violet"
                        if "transparent" in row['NomFR'].casefold() or "translucide" in row['NomFR'].casefold() or "TRA" in clean_id: couleur = "transparent"
                        logging.info("Color '" + couleur + "' inferred from match in either caption or Id ")
                
                # Now solving for size through positional means
                tentative_size = clean_id[0:3].casefold()
                tentative_size_match = True
                match tentative_size:
                    case '000': taille = 'unique'
                    case '001': taille = '1000g'
                    case '015': taille = '1500g'
                    case '006': taille = '6'
                    case '010': taille = '10'
                    case '011': taille = '11'
                    case '012': taille = '12'
                    case '013': taille = '13'
                    case '030': taille = '30l'
                    case '050': 
                        # '050' means 50L for a SAC or size 5 for a GAN
                        if row['Id'][0:3].casefold() == 'sac': taille = '50l'
                        else: taille = '5'
                    case '060': taille = '6'
                    case '070': taille = '7'
                    case '080': taille = '8'
                    case '090': taille = '9'
                    case '0sm': taille = 'm'
                    case 'lxl': taille = 'l'
                    case '1.l': taille = '1l'
                    case '10.': taille = '10_5'
                    case '100': taille = '10'
                    case '10x': taille = '100'
                    case '75x': taille = '75'
                    case '8x5': taille = '85'
                    case '9x5': taille = '90'
                    case '110': 
                        # '110' means 110L for a SAC or size 11 for a GAN
                        if row['Id'][0:3].casefold() == 'sac': taille = '110l'
                        else: taille = '11'
                    case '111': taille = '110l'
                    case '120': taille = '12'
                    case '130': 
                        # '130' means 130L for a SAC or size 13 for a GAN
                        if row['Id'][0:3].casefold() == 'sac': taille = '130l'
                        else: taille = '13'
                    case '150': taille = '150l'
                    case '160': taille = '160l'
                    case '2.l': taille = '2l'
                    case '200': taille = '200l'
                    case '200': taille = '200l'
                    case '2x3': taille = 'xxl'
                    case '2xl': taille = 'xxl'
                    case '2xs': taille = 'xxs'
                    case '245': taille = '245'
                    case '300': taille = '300'
                    case '340': taille = '34'
                    case '350': taille = '35'
                    case '360': taille = '36'
                    case '363': taille = '36'
                    case '370': taille = '37'
                    case '380': taille = '38'
                    case '390': taille = '39'
                    case '3xl': taille = 'xxxl'
                    case '40/': taille = '40'
                    case '400': taille = '40'
                    case '410': taille = '41'
                    case '420': taille = '42'
                    case '430': taille = '43'
                    case '44/': taille = '44'
                    case '440': taille = '44'
                    case '450': taille = '45'
                    case '460': taille = '46'
                    case '470': taille = '47'
                    case '480': taille = '48'
                    case '490': taille = '49'
                    case '4xl': taille = 'xxxxl'
                    case '500': taille = '50'
                    case '520': taille = '52'
                    case '540': taille = '54'
                    case '560': taille = '56'
                    case '580': taille = '58'
                    case '5xl': taille = 'xxxxxl'
                    case '6.5': taille = '6_5'
                    case '600': taille = '60'
                    case '620': taille = '62'
                    case '640': taille = '64'
                    case '6xl': taille = 'xxxxxxl'
                    case '7.5': taille = '7_5'
                    case '750': taille = '900ml'
                    case '8.5': taille = '8_5'
                    case '9.5': taille = '9_5'
                    case '908': taille = '8'
                    case '909': taille = '9'
                    case '910': taille = '10'
                    case 'a2x': taille = 'xxl'
                    case 'a3x': taille = 'xxxl'
                    case 'axl': taille = 'xl'
                    case 'c44': taille = '38'
                    case 'c46': taille = '40'
                    case 'c48': taille = '42'
                    case 'c50': taille = '44'
                    case 'c52': taille = '46'
                    case 'c54': taille = '48'
                    case 'c58': taille = '50'
                    case 'c60': taille = '54'
                    case 'c62': taille = '56'
                    case 'c64': taille = '58'
                    case 'c66': taille = '60'
                    case 'l00': taille = 'l'
                    case 'l0m': taille = 'l'
                    case 'l0n': taille = 'l'
                    case 'l20': taille = 'l'
                    case 'lma': taille = 'l'
                    case 'lxl': taille = 'xl'
                    case 'lxl': taille = 'xl'
                    case 'm/l': taille = 'l'
                    case 'm00': taille = 'm'
                    case 'm05': taille = '5m'
                    case 'm06': taille = '6m'
                    case 'm0m': taille = 'm'
                    case 'm10': taille = '10m'
                    case 'm15': taille = '15m'
                    case 'm20': taille = 'm'
                    case 'p34': taille = '34'
                    case 'p35': taille = '35'
                    case 'p36': taille = '36'
                    case 'p37': taille = '37'
                    case 'p38': taille = '38'
                    case 'p39': taille = '39'
                    case 'p40': taille = '40'
                    case 'p41': taille = '41'
                    case 'p42': taille = '42'
                    case 'p43': taille = '43'
                    case 'p44': taille = '44'
                    case 'p45': taille = '45'
                    case 'p46': taille = '46'
                    case 't46': taille = '46'
                    case 'p47': taille = '47'
                    case 'p48': taille = '48'
                    case 'p49': taille = '49'
                    case 'p50': taille = '50'
                    case 'p51': taille = '51'
                    case 'p52': taille = '52'
                    case 'ps2': taille = '37'
                    case 'ps3': taille = '39'
                    case 'ps4': taille = '41'
                    case 'ps5': taille = '43'
                    case 'ps6': taille = '45'
                    case 'ps7': taille = '47'
                    case 'ps8': taille = '49'
                    case 'pv1': taille = 'm'
                    case 'pv2': taille = 's'
                    case 'pv3': taille = 'xl'
                    case 's/m': taille = 'm'
                    case 's00': taille = 's'
                    case 's04':
                        if row['Id'][13:16] == '005': taille = '4_5'
                        else: taille = '4'
                    case 's05':
                        if row['Id'][13:16] == '005': taille = '5_5'
                        else: taille = '5'
                    case 's06':
                        if row['Id'][13:16] == '005': taille = '6_5'
                        else: taille = '6'
                    case 's07':
                        if row['Id'][13:16] == '005': taille = '7_5'
                        else: taille = '7'
                    case 's08':
                        if row['Id'][13:16] == '005': taille = '8_5'
                        else: taille = '8'
                    case 's09':
                        if row['Id'][13:16] == '005': taille = '9_5'
                        else: taille = '9'
                    case 's10':
                        if row['Id'][13:16] == '005': taille = '10_5'
                        else: taille = '10'
                    case 's11':
                        if row['Id'][13:16] == '005': taille = '11_5'
                        else: taille = '11'
                    case 's12':
                        if row['Id'][13:16] == '005': taille = '12_5'
                        else: taille = '12'
                    case 's13':
                        if row['Id'][13:16] == '005': taille = '13_5'
                        else: taille = '13'
                    case 's14':
                        if row['Id'][13:16] == '005': taille = '14_5'
                        else: taille = '14'
                    case 's20': taille = 's'
                    case 't00': taille = '0'
                    case 't10': taille = '1'
                    case 't1j': taille = '1'
                    case 't20': taille = '2'
                    case 't30': taille = '3'
                    case 't40': taille = '4'
                    case 't50': taille = '5'
                    case 't60': taille = '6'
                    case 'tfe': taille = 'femme'
                    case 'tho': taille = 'homme'
                    case 'xl0': taille = 'xl'
                    case 'xl2': taille = 'xl'
                    case 'xla': taille = 'xl'
                    case 'xlm': taille = 'xl'
                    case 'xs0': taille = 'xs'
                    case 'xxl': taille = 'xxl'
                    case 'xxs': taille = 'xxs'
                    case 'cit': taille = '375ml'
                    case 'fru': taille = '375ml'
                    case 'pro': taille = 'protection'
                    case 'ufo': taille = 'unifocal'
                    case 'uni': taille = 'unifocal'
                    case 'bif': taille = 'bifocal'
                    case 'ble': taille = 'mousse'
                    case 'ver': taille = 'unique'
                    case 'jau': taille = 'unique'
                    
                    case _: tentative_size_match = False
                    
                if tentative_size_match: 
                    # Size found through positional means
                    logging.info("Size '" + taille + "' inferred from positional substring '" + tentative_size + "'")
                else: 
                    # Could not find a size through positional means, finding out size from caption or other parts of Id
                    # Size is a unique attribute, let's set up a unique mock size then try to guess the color from row['NomFR'] or row['Id']
                    taille = "999_" + row['Id'].translate(str.maketrans(".-/\ ", "_____"))
                    logging.info("NO SIZE INFERRED from positional substring '" + tentative_size + "', mock size '" + taille + "' was made up instead'")
                    
            # Checking if the product variant family expects the size attribute
            if famillevariant == "ProduitCouleurTaille" or row['Id'] in force_colorsize_family:
                # This is a product model varying by color and then by size

                ## Check the PIM to see if the size exists
                #response = requests.get(ROOT_URL + '/api/rest/v1/attributes/Taille/options/' + taille, headers=headers)
                #if response.status_code == 200:
                #    # Size exists
                #    logging.info('Size "' + taille + '" already exists.')
                #else:
                #    # Size does not exist, it needs to be created
                #    logging.info('Size "' + taille + '" does not exist. Creating...')
                #    
                #    # Collecting size data
                #    # https://api.akeneo.com/api-reference.html#post_attributes__attribute_code__options
                #    size_data = {
                #        "code": taille,
                #        "attribute": "Taille",
                #        "sort_order": 9999,
                #        "labels": {"fr_FR": None,
                #            "it_IT": None}
                #    }
                #    
                #    response = requests.post(ROOT_URL + '/api/rest/v1/attributes/Taille/options', headers=headers, json=size_data)
                #    # Checking response
                #    if response.status_code == 201:
                #        # Code 201 returned after successful creation
                #        logging.info('Size "' + taille + '" created successfully.')
                #    else:
                #        logging.info('[**ERROR**] : ERROR CREATING SIZE "' + taille + '" : ' + chr(response.status_code) + ' - ' + response.text)
                #        error_log_error_list.append('ERROR CREATING SIZE "' + taille + '" : ' + chr(response.status_code) + ' - ' + response.text)

                    
                # Now we write the data for the color/size varying product
                # Color will be a product model with the root product model as a parent
                # See https://help.akeneo.com/serenity-your-first-steps-with-akeneo/serenity-what-about-products-with-variants
                
                # Collecting sub product model data (the product model that holds the color)
                # https://api.akeneo.com/api-reference.html#Productmodel
                product_submodel_data = {
                    "code": code_racine + "_" + couleur,
                    "family": famille,
                    "family_variant": famillevariant,
                    "parent": code_racine,
                    "categories": categories,
                    "values": {
                        "Nom": [{"locale": "fr_FR", "scope": "ebp", "data": row['NomArticleRacineFR']},
                        {"locale": "it_IT", "scope": "ebp", "data": row['NomArticleRacineIT']},
                        {"locale": "fr_FR", "scope": "prestashop", "data": row['NomArticleRacineFR']},
                        {"locale": "it_IT", "scope": "prestashop", "data": row['NomArticleRacineIT']}],
                        "Description": [{"locale": "fr_FR", "scope": "ebp", "data": row['DescriptionArticleRacineFR']},
                        {"locale": "it_IT", "scope": "ebp", "data": row['DescriptionArticleRacineIT']},
                        {"locale": "fr_FR", "scope": "prestashop", "data": row['DescriptionArticleRacineFR']},
                        {"locale": "it_IT", "scope": "prestashop", "data": row['DescriptionArticleRacineIT']}],
                        "Couleur": [{"locale": None, "scope": None, "data": couleur}],
                    }
                }
                
                # Checking PIM to see if the product submodel exists
                response = requests.get(PRODUCT_MODELS_ENDPOINT + '/' + code_racine + "_" + couleur, headers=headers)
                
                if response.status_code == 200:
                    # Product submodel exists, it's an update
                    logging.info('Product submodel ' + code_racine + "_" + couleur + ' found. Updating product submodel data...')
                    response = requests.patch(PRODUCT_MODELS_ENDPOINT + '/' + code_racine + "_" + couleur, headers=headers, json=product_submodel_data)
                    # Checking response
                    if response.status_code == 204:
                        # Code 204 returned after successful update
                        logging.info('Product submodel ' + code_racine + "_" + couleur + ' updated successfully.')
                    else:
                        logging.info('[**ERROR**] : ERROR UPDATING PRODUCT SUBMODEL ' + code_racine + "_" + couleur + ' : ' + chr(response.status_code) + ' - ' + response.text)
                        error_log_error_list.append('ERROR UPDATING PRODUCT SUBMODEL ' + code_racine + "_" + couleur + ' : ' + chr(response.status_code) + ' - ' + response.text)
                else:
                    # Product submodel does not exist, it needs to be created
                    logging.info('Product submodel ' + code_racine + "_" + couleur + ' not found. Creating product submodel...')
                    response = requests.post(PRODUCT_MODELS_ENDPOINT, headers=headers, json=product_submodel_data)
                    # Checking response
                    if response.status_code == 201:
                        # Code 201 returned after successful creation
                        logging.info('Product submodel ' + code_racine + "_" + couleur + ' created successfully.')
                    else:
                        logging.info('[**ERROR**] : ERROR CREATING PRODUCT SUBMODEL ' + code_racine + "_" + couleur + ' : ' + chr(response.status_code) + ' - ' + response.text)
                        error_log_error_list.append('ERROR CREATING PRODUCT SUBMODEL ' + code_racine + "_" + couleur + ' : ' + chr(response.status_code) + ' - ' + response.text)
                
                # Collecting current product data (color and size variant)
                # https://api.akeneo.com/api-reference.html#Productidentifier
                product_data = {
                    "identifier": row['Id'],
                    "family": famille,
                    "categories": categories,
                    "parent": code_racine + "_" + couleur,
                    "enabled": enabled,
                    "values": {
                        "Nom": [{"locale": "fr_FR", "scope": "ebp", "data": row['NomFR']},
                        {"locale": "it_IT", "scope": "ebp", "data": row['NomIT']},
                        {"locale": "fr_FR", "scope": "prestashop", "data": row['NomFR']},
                        {"locale": "it_IT", "scope": "prestashop", "data": row['NomIT']}],
                        "Description": [{"locale": "fr_FR", "scope": "ebp", "data": row['DescriptionFR']},
                        {"locale": "it_IT", "scope": "ebp", "data": row['DescriptionIT']}],
                        "Couleur": [{"locale": None, "scope": None, "data": couleur}],
                        "Taille": [{"locale": None, "scope": None, "data": taille}],
                        "Publier": [{"locale": None, "scope": "prestashop", "data": enabled}],
                        "DateDernierePublication": [{"locale": None, "scope": "prestashop", "data": None}],
                        "PrixHT": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixVenteHT']),"currency": "EUR"}]}],
                        "PrixTTC": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixVenteTTC']),"currency": "EUR"}]}],
                        "DescriptionTVA": [{"locale": None, "scope": None, "data": row['DescriptionTVA']}],
                        "TauxTVA": [{"locale": None, "scope": None, "data": str(row['TauxTVA'])}],
                        "MontantTVA": [{"locale": None, "scope": None, "data": [{"amount": str(row['MontantTVA']),"currency": "EUR"}]}],
                        "PrixAchat": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixAchat']),"currency": "EUR"}]}],
                        "TauxCharges": [{"locale": None, "scope": None, "data": str(row['TauxCharges'])}],
                        "MontantCharges": [{"locale": None, "scope": None, "data": [{"amount": str(row['MontantCharges']),"currency": "EUR"}]}],
                        "PrixRevient": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixRevient']),"currency": "EUR"}]}],
                        "MontantMargeNette": [{"locale": None, "scope": None, "data": [{"amount": str(row['MontantMargeNette']),"currency": "EUR"}]}],
                        "TauxMargeNette": [{"locale": None, "scope": None, "data": str(row['TauxMargeNette'])}],
                        "TauxMarque": [{"locale": None, "scope": None, "data": str(row['TauxMarque'])}],
                        "QuantiteParDefaut": [{"locale": None, "scope": None, "data": str(row['QuantiteParDefaut'])}],
                        "QuantiteParConditionnement": [{"locale": None, "scope": None, "data": str(row['QuantiteParConditionnement'])}],
                        "ConditionnementMinimal": [{"locale": None, "scope": None, "data": str(row['ConditionnementMinimal'])}],
                        "Fournisseur": [{"locale": None, "scope": None, "data": str(row['Fournisseur'])}],
                        "ReferenceFournisseur": [{"locale": None, "scope": None, "data": str(row['ReferenceFournisseur'])}],
                        "NomProduitFournisseur": [{"locale": None, "scope": None, "data": str(row['NomProduitFournisseur'])}],
                        "CodeBarre": [{"locale": None, "scope": None, "data": str(row['CodeBarre'])}],
                        "DelaiLivraisonFournisseur": [{"locale": None, "scope": None, "data": str(row['DelaiLivraisonFournisseur'])}],
                        "Longueur": [{"locale": None, "scope": None, "data": {"amount": str(row['Longueur']),"unit": "MILLIMETER"}}],
                        "Largeur": [{"locale": None, "scope": None, "data": {"amount": str(row['Largeur']),"unit": "MILLIMETER"}}],
                        "Hauteur": [{"locale": None, "scope": None, "data": {"amount": str(row['Hauteur']),"unit": "MILLIMETER"}}],
                        "Poids": [{"locale": None, "scope": None, "data": {"amount": str(row['Poids']),"unit": "GRAM"}}],
                        "PoidsNet": [{"locale": None, "scope": None, "data": {"amount": str(row['PoidsNet']),"unit": "GRAM"}}],
                        "Contremarque": [{"locale": None, "scope": None, "data": contremarque}],
                        "StockReel": [{"locale": None, "scope": None, "data": str(row['StockReel'])}],
                        "PrixMoyenPondereStockReel": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixMoyenPondereStockReel']),"currency": "EUR"}]}],
                        "ValeurStockReel": [{"locale": None, "scope": None, "data": [{"amount": str(row['ValeurStockReel']),"currency": "EUR"}]}],
                        "StockVirtuel": [{"locale": None, "scope": None, "data": str(row['StockVirtuel'])}],
                        "PrixMoyenPondereStockVirtuel": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixMoyenPondereStockVirtuel']),"currency": "EUR"}]}],
                        "ValeurStockVirtuel": [{"locale": None, "scope": None, "data": [{"amount": str(row['ValeurStockVirtuel']),"currency": "EUR"}]}],
                        "StockReelConforme": [{"locale": None, "scope": None, "data": str(row['StockReelConforme'])}],
                        "QuantiteReservee": [{"locale": None, "scope": None, "data": str(row['QuantiteReservee'])}],
                        "QuantitePrepaBL": [{"locale": None, "scope": None, "data": str(row['QuantitePrepaBL'])}],
                    }
                }
                
                # Checking PIM to see if the product exists
                response = requests.get(PRODUCTS_ENDPOINT + '/' + row['Id'], headers=headers)
                
                if response.status_code == 200:
                    # Product exists, it's an update
                    logging.info('Product ' + row['Id'] + ' found. Updating product data...')
                    response = requests.patch(PRODUCTS_ENDPOINT + '/' + row['Id'], headers=headers, json=product_data)
                    # Checking response
                    if response.status_code == 204:
                        # Code 204 returned after successful update
                        logging.info('Product ' + row['Id'] + ' updated successfully.')
                    else:
                        logging.info('[**ERROR**] : ERROR UPDATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
                        error_log_error_list.append('ERROR UPDATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
                else:
                    # Product does not exist, it needs to be created
                    logging.info('Product ' + row['Id'] + ' not found. Creating product...')
                    response = requests.post(PRODUCTS_ENDPOINT, headers=headers, json=product_data)
                    # Checking response
                    if response.status_code == 201:
                        # Code 201 returned after successful creation
                        logging.info('Product ' + row['Id'] + ' created successfully.')
                    else:
                        logging.info('[**ERROR**] : ERROR CREATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
                        error_log_error_list.append('ERROR CREATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
                    
            else:
            # This is a color only product
            
                # Collecting current product data (color only variant)
                # https://api.akeneo.com/api-reference.html#Productidentifier
                product_data = {
                    "identifier": row['Id'],
                    "family": famille,
                    "categories": categories,
                    "parent": code_racine,
                    "enabled": enabled,
                    "values": {
                        "Nom": [{"locale": "fr_FR", "scope": "ebp", "data": row['NomFR']},
                        {"locale": "it_IT", "scope": "ebp", "data": row['NomIT']},
                        {"locale": "fr_FR", "scope": "prestashop", "data": row['NomFR']},
                        {"locale": "it_IT", "scope": "prestashop", "data": row['NomIT']}],
                        "Description": [{"locale": "fr_FR", "scope": "ebp", "data": row['DescriptionFR']},
                        {"locale": "it_IT", "scope": "ebp", "data": row['DescriptionIT']},
                        {"locale": "fr_FR", "scope": "prestashop", "data": row['DescriptionFR']},
                        {"locale": "it_IT", "scope": "prestashop", "data": row['DescriptionIT']}],
                        "Couleur": [{"locale": None, "scope": None, "data": couleur}],
                        "Publier": [{"locale": None, "scope": "prestashop", "data": enabled}],
                        "DateDernierePublication": [{"locale": None, "scope": "prestashop", "data": None}],
                        "PrixHT": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixVenteHT']),"currency": "EUR"}]}],
                        "PrixTTC": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixVenteTTC']),"currency": "EUR"}]}],
                        "DescriptionTVA": [{"locale": None, "scope": None, "data": row['DescriptionTVA']}],
                        "TauxTVA": [{"locale": None, "scope": None, "data": str(row['TauxTVA'])}],
                        "MontantTVA": [{"locale": None, "scope": None, "data": [{"amount": str(row['MontantTVA']),"currency": "EUR"}]}],
                        "PrixAchat": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixAchat']),"currency": "EUR"}]}],
                        "TauxCharges": [{"locale": None, "scope": None, "data": str(row['TauxCharges'])}],
                        "MontantCharges": [{"locale": None, "scope": None, "data": [{"amount": str(row['MontantCharges']),"currency": "EUR"}]}],
                        "PrixRevient": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixRevient']),"currency": "EUR"}]}],
                        "MontantMargeNette": [{"locale": None, "scope": None, "data": [{"amount": str(row['MontantMargeNette']),"currency": "EUR"}]}],
                        "TauxMargeNette": [{"locale": None, "scope": None, "data": str(row['TauxMargeNette'])}],
                        "TauxMarque": [{"locale": None, "scope": None, "data": str(row['TauxMarque'])}],
                        "QuantiteParDefaut": [{"locale": None, "scope": None, "data": str(row['QuantiteParDefaut'])}],
                        "QuantiteParConditionnement": [{"locale": None, "scope": None, "data": str(row['QuantiteParConditionnement'])}],
                        "ConditionnementMinimal": [{"locale": None, "scope": None, "data": str(row['ConditionnementMinimal'])}],
                        "Fournisseur": [{"locale": None, "scope": None, "data": str(row['Fournisseur'])}],
                        "ReferenceFournisseur": [{"locale": None, "scope": None, "data": str(row['ReferenceFournisseur'])}],
                        "NomProduitFournisseur": [{"locale": None, "scope": None, "data": str(row['NomProduitFournisseur'])}],
                        "CodeBarre": [{"locale": None, "scope": None, "data": str(row['CodeBarre'])}],
                        "DelaiLivraisonFournisseur": [{"locale": None, "scope": None, "data": str(row['DelaiLivraisonFournisseur'])}],
                        "Longueur": [{"locale": None, "scope": None, "data": {"amount": str(row['Longueur']),"unit": "MILLIMETER"}}],
                        "Largeur": [{"locale": None, "scope": None, "data": {"amount": str(row['Largeur']),"unit": "MILLIMETER"}}],
                        "Hauteur": [{"locale": None, "scope": None, "data": {"amount": str(row['Hauteur']),"unit": "MILLIMETER"}}],
                        "Poids": [{"locale": None, "scope": None, "data": {"amount": str(row['Poids']),"unit": "GRAM"}}],
                        "PoidsNet": [{"locale": None, "scope": None, "data": {"amount": str(row['PoidsNet']),"unit": "GRAM"}}],
                        "Contremarque": [{"locale": None, "scope": None, "data": contremarque}],
                        "StockReel": [{"locale": None, "scope": None, "data": str(row['StockReel'])}],
                        "PrixMoyenPondereStockReel": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixMoyenPondereStockReel']),"currency": "EUR"}]}],
                        "ValeurStockReel": [{"locale": None, "scope": None, "data": [{"amount": str(row['ValeurStockReel']),"currency": "EUR"}]}],
                        "StockVirtuel": [{"locale": None, "scope": None, "data": str(row['StockVirtuel'])}],
                        "PrixMoyenPondereStockVirtuel": [{"locale": None, "scope": None, "data": [{"amount": str(row['PrixMoyenPondereStockVirtuel']),"currency": "EUR"}]}],
                        "ValeurStockVirtuel": [{"locale": None, "scope": None, "data": [{"amount": str(row['ValeurStockVirtuel']),"currency": "EUR"}]}],
                        "StockReelConforme": [{"locale": None, "scope": None, "data": str(row['StockReelConforme'])}],
                        "QuantiteReservee": [{"locale": None, "scope": None, "data": str(row['QuantiteReservee'])}],
                        "QuantitePrepaBL": [{"locale": None, "scope": None, "data": str(row['QuantitePrepaBL'])}],
                    }
                }
                
                # Adding the size if it has been found
                if tentative_size_match: product_data['values']['Taille'] = [{"locale": None, "scope": None, "data": taille}]
                
                # Checking PIM to see if the product exists
                response = requests.get(PRODUCTS_ENDPOINT + '/' + row['Id'], headers=headers)
                
                if response.status_code == 200:
                    # Product exists, it's an update
                    logging.info('Product ' + row['Id'] + ' found. Updating product data...')
                    response = requests.patch(PRODUCTS_ENDPOINT + '/' + row['Id'], headers=headers, json=product_data)
                    # Checking response
                    if response.status_code == 204:
                        # Code 204 returned after successful update
                        logging.info('Product ' + row['Id'] + ' updated successfully.')
                    else:
                        logging.info('[**ERROR**] : ERROR UPDATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
                        error_log_error_list.append('ERROR UPDATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
                else:
                    # Product does not exist, it needs to be created
                    logging.info('Product ' + row['Id'] + ' not found. Creating product...')
                    response = requests.post(PRODUCTS_ENDPOINT, headers=headers, json=product_data)
                    # Checking response
                    if response.status_code == 201:
                        # Code 201 returned after successful creation
                        logging.info('Product ' + row['Id'] + ' created successfully.')
                    else:
                        logging.info('[**ERROR**] : ERROR CREATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)
                        error_log_error_list.append('ERROR CREATING PRODUCT ' + row['Id'] + ' : ' + chr(response.status_code) + ' - ' + response.text)

    if error_log_error_list:
        # Making the HTML Content for the log mail
        html_content= f"""
            <p>This is an automated message from <a href="https://apache-airflow.groupe-fjc.com/">apache-airflow.groupe-fjc.com/</a></p>
            <br>
            <p>products_ebp_akeneo has encountered the following errors :</p>
            <ul type="square">
            """
        for errorindex in range(len(error_log_error_list)):
            html_content+= """<li>""" + error_log_error_list[errorindex] + """</li>"""
        html_content+= f"""
        </ul>
        <p>Thank you.</p>
        <br>"""
        
        #Sending the log mail
        for recipientindex in range(len(error_log_mailing_list)):
            send_email(
                to=str(error_log_mailing_list[recipientindex]),
                subject='Airflow DAG products_ebp_akeneo - error log',
                html_content=html_content
            )


# PythonOperator to run the data orchestration code
fetch_data_task = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data_and_iterate,
    dag=dag
)

# Set task dependencies
fetch_data_task

if __name__ == "__main__":
    dag.cli()
