###
# Author : Lewis Chavas
###
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import base64
import requests
import json
import time
import logging
import html.entities

# Default DAG parameters
default_args = {
    'owner': 'Lewis Chavas',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

# Instantiate the DAG
dag = DAG(
    'products_to_prestashop',
    default_args=default_args,
    description='Sync products to Prestashop',
    max_active_runs=1,
    catchup=False,
    schedule_interval="@hourly",
    tags=["pim"],
)

# Function that will be called by PythonOperator 'fetch_data_task'
def fetch_data_and_iterate(prev_execution_date=None):

    # PrestaShop API credentials
    PRESTASHOP_API_URL = "https://prestashop.groupe-fjc.com/api"
    PRESTASHOP_API_KEY = ""
    
    # Setting up the translation table for html elements (é>&eagrave; è>&eacute; ç>&ccedil; and so on)
    htmltable = {k: '&{};'.format(v) for k, v in html.entities.codepoint2name.items()}
    
    # This object will contain the default headers 
    prestashop_headers = None
    
    # Setting up API request prestashop_headers
    prestashop_headers = {
        "Authorization": 'Basic ' + base64.b64encode(f"{PRESTASHOP_API_KEY}:".encode("utf-8")).decode("utf-8"),
        "Content-Type": "application/json",
    }
    
    def urlify(string):
        return string.replace(' ', '-').lower().encode("ascii", errors="ignore").decode()
    
    ##################################
    # MANUFACTURER HANDLING FUNCTIONS
    # SEE https://devdocs.prestashop-project.org/8/webservice/resources/manufacturers/
    ##################################
    
    def get_manufacturer_id_by_name(name):
        debugurl=PRESTASHOP_API_URL + '/manufacturers?filter[name]=' + name + "&output_format=JSON"
        prestashop_response = requests.get(PRESTASHOP_API_URL + '/manufacturers?filter[name]=' + name + "&output_format=JSON", headers=prestashop_headers)
        if prestashop_response.status_code == 200:
            response= prestashop_response.json()
            if isinstance(response, list) :
                #no data, empty list returned
                return None
            else:
                manufacturers = prestashop_response.json().get('manufacturers', [])
                if manufacturers:
                    return manufacturers[0]['id']
                else:
                    return None
        else:
            logging.error(f"Error while getting id for manufacturer '{name}': {prestashop_response.status_code}, {prestashop_response.text}")
            return None
            
    def create_or_update_manufacturer(
        name,active,descriptionFR,descriptionIT,short_descriptionFR,short_descriptionIT,
        meta_titleFR,meta_titleIT,meta_keywordsFR,meta_keywordsIT,meta_descriptionFR,meta_descriptionIT):
        
        #check if exists
        id = get_manufacturer_id_by_name(name)
        # Manufacturer ("Marque" in Prestashop) XML data with all dynamic fields
        id_node = f"{'<id>'+str(id)+'</id>' if id else ''}"
        manufacturer_xml = f"""
        <prestashop xmlns:xlink="http://www.w3.org/1999/xlink">
            <manufacturer>
                {id_node}
                <active>{active}</active>
                <name><![CDATA[{name}]]></name>
                <description>
                    <language id="1"><![CDATA[{descriptionFR.translate(htmltable)}]]></language>
                    <language id="2"><![CDATA[{descriptionIT.translate(htmltable)}]]></language>
                </description>
                <short_description>
                    <language id="1"><![CDATA[{short_descriptionFR.translate(htmltable)}]]></language>
                    <language id="2"><![CDATA[{short_descriptionIT.translate(htmltable)}]]></language>
                </short_description>
                <meta_title>
                    <language id="1"><![CDATA[{meta_titleFR}]]></language>
                    <language id="2"><![CDATA[{meta_titleIT}]]></language>
                </meta_title>
                <meta_keywords>
                    <language id="1"><![CDATA[{meta_keywordsFR}]]></language>
                    <language id="2"><![CDATA[{meta_keywordsIT}]]></language>
                </meta_keywords>
                <meta_description>
                    <language id="1"><![CDATA[{meta_descriptionFR}]]></language>
                    <language id="2"><![CDATA[{meta_descriptionIT}]]></language>
                </meta_description>
            </manufacturer>
        </prestashop>
        """

        if id:
        #we update the manufacturer
            prestashop_response = requests.patch(PRESTASHOP_API_URL + '/manufacturers' + "?output_format=JSON", data=manufacturer_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 200:
                logging.info(f"Manufacturer '{name}' updated successfully: {prestashop_response.text}")
            else:
                logging.error(f"Failed to update manufacturer '{name}': {prestashop_response.status_code}, {prestashop_response.text}")
        
        else:
        #we create the manufacturer
            prestashop_response = requests.post(PRESTASHOP_API_URL + '/manufacturers' + "?output_format=JSON", data=manufacturer_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 201:
                logging.info(f"Manufacturer '{name}' created successfully: {prestashop_response.text}")
                # grab the manufacturer id
                id = prestashop_response.json()['manufacturer']['id']
            else:
                logging.error(f"Failed to create manufacturer '{name}': {prestashop_response.status_code}, {prestashop_response.text}")
        return id
        
    def set_manufacturer_logo(manufacturer_id, image_url):
        # Download the image from the given URL
        image_data = requests.get(image_url).content

        # Prepare the image file for the request
        files = {'image': ('downloaded_image.jpg', image_data, 'image/jpg')}
        
        # Encode the files for the request
        logo_body, logo_content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
        
        # Prepare headers
        logo_header = {
            "Authorization": 'Basic ' + base64.b64encode(f"{PRESTASHOP_API_KEY}:".encode("utf-8")).decode("utf-8"),
            "Content-Type": logo_content_type,
        }

        # Check if the image already exists
        check_response = requests.head(
            f"{PRESTASHOP_API_URL}/images/manufacturers/{manufacturer_id}?output_format=JSON",
            headers={"Authorization": 'Basic ' + base64.b64encode(f"{PRESTASHOP_API_KEY}:".encode("utf-8")).decode("utf-8")}
        )

        if check_response.status_code == 404 or check_response.status_code == 500 :
            # Image does not exist; create it with POST
            prestashop_response = requests.post(
                f"{PRESTASHOP_API_URL}/images/manufacturers/{manufacturer_id}?output_format=JSON",
                data=logo_body,
                headers=logo_header
            )
        elif check_response.status_code == 200:
            # Image exists; update it with POST and &ps_method=PUT
            prestashop_response = requests.post(
                f"{PRESTASHOP_API_URL}/images/manufacturers/{manufacturer_id}?output_format=JSON&ps_method=PUT",
                data=logo_body,
                headers=logo_header
            )
        else:
            logging.error(f"Failed to check if manufacturer image exists: {check_response.status_code}, {check_response.text}")
            return

        # Handle the response
        if prestashop_response.status_code == 200:
            logging.info(f"Manufacturer '{manufacturer_id}': logo {('updated' if '&ps_method=PUT' in prestashop_response.url else 'created')} successfully!")
        else:
            logging.error(f"Failed to {('update' if '&ps_method=PUT' in prestashop_response.url else 'create')} manufacturer logo: {prestashop_response.status_code}, {prestashop_response.text}")
   
    #def set_manufacturer_logo(manufacturer_id, image_url):
    #    # Download the image from the given URL
    #    image_data = requests.get(image_url).content
    #    
    #    # Prepare the image file for the request
    #    files = {'image': ('downloaded_image.jpg', image_data, 'image/jpg')}
    #    
    #    # Encode the files for the request
    #    logo_body, logo_content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
    #    
    #    # Prepare headers
    #    logo_header = {
    #        "Authorization": 'Basic ' + base64.b64encode(f"{PRESTASHOP_API_KEY}:".encode("utf-8")).decode("utf-8"),
    #        "Content-Type": logo_content_type,
    #    }
    #    
    #    # Send the POST request to update the manufacturer logo
    #    prestashop_response = requests.post(PRESTASHOP_API_URL + '/images/manufacturers/' + str(manufacturer_id) + "?output_format=JSON",data=logo_body,headers=logo_header)
    #    
    #    # Handle the response
    #    if prestashop_response.status_code == 200:
    #        logging.info(f"Manufacturer '{manufacturer_id}': logo updated successfully!")
    #    else:
    #        logging.error(f"Failed to update manufacturer logo: {prestashop_response.status_code}, {prestashop_response.text}")
        
    ##############################
    # SUPPLIER HANDLING FUNCTIONS
    # SEE https://devdocs.prestashop-project.org/8/webservice/resources/suppliers/
    # NOTE : THE SUPPLIER MUST HAVE AN ADDRESS
    ##############################
        
    def get_supplier_id_by_name(name):
        prestashop_response = requests.get(PRESTASHOP_API_URL + '/suppliers?filter[name]=' + name + "&output_format=JSON", headers=prestashop_headers)
        if prestashop_response.status_code == 200:
            response= prestashop_response.json()
            if isinstance(response, list) :
                #no data, empty list returned
                return None
            else:
                suppliers = prestashop_response.json().get('suppliers', [])
                if suppliers:
                    return suppliers[0]['id']
                else:
                    return None
        else:
            logging.error(f"Error while getting id for supplier '{name}': {prestashop_response.status_code}, {prestashop_response.text}")
            return None
            
    def get_supplier_address_by_supplier_id(supplier_id):
        prestashop_response = requests.get(PRESTASHOP_API_URL + '/addresses?filter[id_supplier]=' + str(supplier_id) + "&output_format=JSON", headers=prestashop_headers)
        if prestashop_response.status_code == 200:
            response= prestashop_response.json()
            if isinstance(response, list) :
                #no data, empty list returned
                return None
            else:
                addresses = prestashop_response.json().get('addresses', [])
                if addresses:
                    return addresses[0]['id']
                else:
                    return None
        else:
            logging.error(f"Error while getting address id for supplier #'{supplier_id}': {prestashop_response.status_code}, {prestashop_response.text}")
            return None
            
    def create_or_update_supplier(
    name,active,descriptionFR,descriptionIT,meta_titleFR,meta_titleIT,meta_keywordsFR,meta_keywordsIT,
    meta_descriptionFR,meta_descriptionIT):
    
        #check if exists
        id = get_supplier_id_by_name(name)
        # supplier ("Marque" in Prestashop) XML data with all dynamic fields
        id_node = f"{'<id>'+str(id)+'</id>' if id else ''}"
        supplier_xml = f"""
        <prestashop xmlns:xlink="http://www.w3.org/1999/xlink">
            <supplier>
                {id_node}
                <active>{active}</active>
                <name><![CDATA[{name}]]></name>
                <description>
                    <language id="1"><![CDATA[{descriptionFR}]]></language>
                    <language id="2"><![CDATA[{descriptionIT}]]></language>
                </description>
                <meta_title>
                    <language id="1"><![CDATA[{meta_titleFR}]]></language>
                    <language id="2"><![CDATA[{meta_titleIT}]]></language>
                </meta_title>
                <meta_keywords>
                    <language id="1"><![CDATA[{meta_keywordsFR}]]></language>
                    <language id="2"><![CDATA[{meta_keywordsIT}]]></language>
                </meta_keywords>
                <meta_description>
                    <language id="1"><![CDATA[{meta_descriptionFR}]]></language>
                    <language id="2"><![CDATA[{meta_descriptionIT}]]></language>
                </meta_description>
            </supplier>
        </prestashop>
        """
        
        if id:
        #we update the supplier
            prestashop_response = requests.patch(PRESTASHOP_API_URL + '/suppliers' + "?output_format=JSON", data=supplier_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 200:
                logging.info(f"Supplier '{name}' updated successfully: {prestashop_response.text}")
            else:
                logging.error(f"Failed to update supplier '{name}': {prestashop_response.status_code}, {prestashop_response.text}")
        
        else:
        #we create the supplier
            logging.info(f"we create the supplier '{name}'")
            prestashop_response = requests.post(PRESTASHOP_API_URL + '/suppliers' + "?output_format=JSON", data=supplier_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 201:
                logging.info(f"Supplier '{name}' created successfully: {prestashop_response.text}")
                # grab the supplier id
                id = prestashop_response.json()['supplier']['id']
            else:
                logging.error(f"Failed to create supplier '{name}': {prestashop_response.status_code}, {prestashop_response.text}")
        return id
        
    def create_or_update_supplier_address(
        supplier_id,address_active,address_alias,address_company,address_firstname,address_lastname,
        address_address1,address_address2,address_postcode,address_city,address_other,address_country,
        address_state,address_phone,address_phone_mobile,address_dni):
        
        address_id = get_supplier_address_by_supplier_id(supplier_id)
        id_node = f"{'<id>'+str(address_id)+'</id>' if address_id else ''}"
        supplier_address_xml = f"""
        <prestashop xmlns:xlink="http://www.w3.org/1999/xlink">
            <address>
                {id_node}
                <active><![CDATA[{address_active}]]></active>
                <alias><![CDATA[{address_alias}]]></alias>
                <company><![CDATA[{address_company}]]></company>
                <firstname><![CDATA[{address_firstname}]]></firstname>
                <lastname><![CDATA[{address_lastname}]]></lastname>
                <address1><![CDATA[{address_address1}]]></address1>
                <address2><![CDATA[{address_address2}]]></address2>
                <postcode><![CDATA[{address_postcode}]]></postcode>
                <city><![CDATA[{address_city}]]></city>
                <other><![CDATA[{address_other}]]></other>
                <id_country>{address_country}</id_country>
                <phone><![CDATA[{address_phone}]]></phone>
                <phone_mobile><![CDATA[{address_phone_mobile}]]></phone_mobile>
                <dni><![CDATA[{address_dni}]]></dni>
                <id_supplier>{supplier_id}</id_supplier>
            </address>
        </prestashop>
        """
        if address_id:
            #we update the address
            logging.info(f"we update the supplier address '{address_alias}'")
            prestashop_response = requests.patch(PRESTASHOP_API_URL + '/addresses' + "?output_format=JSON", data=supplier_address_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 200:
                logging.info(f"Supplier address '{address_alias}' updated successfully: {prestashop_response.text}")
            else:
                logging.error(f"Failed to update supplier address '{address_alias}': {prestashop_response.status_code}, {prestashop_response.text}")
        else:
            #we create the address
            logging.info(f"we create the supplier address '{address_alias}'")
            prestashop_response = requests.post(PRESTASHOP_API_URL + '/addresses' + "?output_format=JSON", data=supplier_address_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 201:
                logging.info(f"Supplier address '{address_alias}' created successfully: {prestashop_response.text}")
            else:
                logging.error(f"Failed to create supplier address '{address_alias}': {prestashop_response.status_code}, {prestashop_response.text}")
    
    def set_supplier_logo(supplier_id, image_url):
        # Download the image from the given URL
        image_data = requests.get(image_url).content

        # Prepare the image file for the request
        files = {'image': ('downloaded_image.jpg', image_data, 'image/jpg')}
        
        # Encode the files for the request
        logo_body, logo_content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
        
        # Prepare headers
        logo_header = {
            "Authorization": 'Basic ' + base64.b64encode(f"{PRESTASHOP_API_KEY}:".encode("utf-8")).decode("utf-8"),
            "Content-Type": logo_content_type,
        }

        # Check if the image already exists
        check_response = requests.head(
            f"{PRESTASHOP_API_URL}/images/suppliers/{supplier_id}?output_format=JSON",
            headers={"Authorization": 'Basic ' + base64.b64encode(f"{PRESTASHOP_API_KEY}:".encode("utf-8")).decode("utf-8")}
        )

        if check_response.status_code == 404 or check_response.status_code == 500 :
            # Image does not exist; create it with POST
            prestashop_response = requests.post(
                f"{PRESTASHOP_API_URL}/images/suppliers/{supplier_id}?output_format=JSON",
                data=logo_body,
                headers=logo_header
            )
        elif check_response.status_code == 200:
            # Image exists; update it with POST and &ps_method=PUT
            prestashop_response = requests.post(
                f"{PRESTASHOP_API_URL}/images/suppliers/{supplier_id}?output_format=JSON&ps_method=PUT",
                data=logo_body,
                headers=logo_header
            )
        else:
            logging.error(f"Failed to check if supplier image exists: {check_response.status_code}, {check_response.text}")
            return

        # Handle the response
        if prestashop_response.status_code == 200:
            logging.info(f"Supplier '{supplier_id}': logo {('updated' if '&ps_method=PUT' in prestashop_response.url else 'created')} successfully!")
        else:
            logging.error(f"Failed to {('update' if '&ps_method=PUT' in prestashop_response.url else 'create')} supplier logo: {prestashop_response.status_code}, {prestashop_response.text}")
    

    ##############################
    # CATEGORY HANDLING FUNCTIONS
    # SEE https://devdocs.prestashop-project.org/8/webservice/resources/categories/
    # NOTES : 
    ##############################

    def get_category_id_by_name(name):
        prestashop_response = requests.get(PRESTASHOP_API_URL + '/categories?filter[name]=' + name + "&output_format=JSON", headers=prestashop_headers)
        if prestashop_response.status_code == 200:
            response= prestashop_response.json()
            if isinstance(response, list) :
                #no data, empty list returned
                return None
            else:
                categories = prestashop_response.json().get('categories', [])
                if categories:
                    return categories[0]['id']
                else:
                    return None
        else:
            logging.error(f"Error while getting id for category '{name}': {prestashop_response.status_code}, {prestashop_response.text}")
            return None
            
    def create_or_update_category(
        parent_id,nameFR,nameIT,active,descriptionFR,descriptionIT,link_rewriteFR,link_rewriteIT,
        meta_titleFR,meta_titleIT,meta_keywordsFR,meta_keywordsIT,meta_descriptionFR,meta_descriptionIT):
        
        #check if exists
        id = get_category_id_by_name(nameFR)
        # category ("Marque" in Prestashop) XML data with all dynamic fields
        id_node = f"{'<id>'+str(id)+'</id>' if id else ''}"
        category_xml = f"""
        <prestashop xmlns:xlink="http://www.w3.org/1999/xlink">
            <category>
                {id_node}
                <id_parent>{parent_id}</id_parent>
                <active>{active}</active>
                <name>
                    <language id="1"><![CDATA[{nameFR}]]></language>
                    <language id="2"><![CDATA[{nameIT}]]></language>
                </name>
                <description>
                    <language id="1"><![CDATA[{descriptionFR}]]></language>
                    <language id="2"><![CDATA[{descriptionIT}]]></language>
                </description>
                <meta_title>
                    <language id="1"><![CDATA[{meta_titleFR}]]></language>
                    <language id="2"><![CDATA[{meta_titleIT}]]></language>
                </meta_title>
                <meta_keywords>
                    <language id="1"><![CDATA[{meta_keywordsFR}]]></language>
                    <language id="2"><![CDATA[{meta_keywordsIT}]]></language>
                </meta_keywords>
                <meta_description>
                    <language id="1"><![CDATA[{meta_descriptionFR}]]></language>
                    <language id="2"><![CDATA[{meta_descriptionIT}]]></language>
                </meta_description>
                <link_rewrite>
                    <language id="1"><![CDATA[{link_rewriteFR}]]></language>
                    <language id="2"><![CDATA[{link_rewriteIT}]]></language>
                </link_rewrite>
            </category>
        </prestashop>
        """
        
        if id:
        #we update the category
            prestashop_response = requests.patch(PRESTASHOP_API_URL + '/categories' + "?output_format=JSON", data=category_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 200:
                logging.info(f"Category '{nameFR}' updated successfully: {prestashop_response.text}")
            else:
                logging.error(f"Failed to update category '{nameFR}': {prestashop_response.status_code}, {prestashop_response.text}")
        
        else:
        #we create the category
            logging.info(f"we create the category '{nameFR}'")
            prestashop_response = requests.post(PRESTASHOP_API_URL + '/categories' + "?output_format=JSON", data=category_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 201:
                logging.info(f"Category '{nameFR}' created successfully: {prestashop_response.text}")
                # grab the category id
                id = prestashop_response.json()['category']['id']
            else:
                logging.error(f"Failed to create category '{nameFR}': {prestashop_response.status_code}, {prestashop_response.text}")
        return id
    
    def set_category_logo(category_id, image_url):
        # Download the image from the given URL
        image_data = requests.get(image_url).content

        # Prepare the image file for the request
        files = {'image': ('downloaded_image.jpg', image_data, 'image/jpg')}
        
        # Encode the files for the request
        logo_body, logo_content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
        
        # Prepare headers
        logo_header = {
            "Authorization": 'Basic ' + base64.b64encode(f"{PRESTASHOP_API_KEY}:".encode("utf-8")).decode("utf-8"),
            "Content-Type": logo_content_type,
        }

        # Check if the image already exists
        check_response = requests.head(
            f"{PRESTASHOP_API_URL}/images/categories/{category_id}?output_format=JSON",
            headers={"Authorization": 'Basic ' + base64.b64encode(f"{PRESTASHOP_API_KEY}:".encode("utf-8")).decode("utf-8")}
        )

        if check_response.status_code == 404 or check_response.status_code == 500 :
            # Image does not exist; create it with POST
            prestashop_response = requests.post(
                f"{PRESTASHOP_API_URL}/images/categories/{category_id}?output_format=JSON",
                data=logo_body,
                headers=logo_header
            )
        elif check_response.status_code == 200:
            # Image exists; update it with POST and &ps_method=PUT
            prestashop_response = requests.post(
                f"{PRESTASHOP_API_URL}/images/categories/{category_id}?output_format=JSON&ps_method=PUT",
                data=logo_body,
                headers=logo_header
            )
        else:
            logging.error(f"Failed to check if category image exists: {check_response.status_code}, {check_response.text}")
            return

        # Handle the response
        if prestashop_response.status_code == 200:
            logging.info(f"Category '{category_id}': logo {('updated' if '&ps_method=PUT' in prestashop_response.url else 'created')} successfully!")
        else:
            logging.error(f"Failed to {('update' if '&ps_method=PUT' in prestashop_response.url else 'create')} category logo: {prestashop_response.status_code}, {prestashop_response.text}")
    
    ##############################
    # PRODUCT HANDLING FUNCTIONS
    # SEE https://devdocs.prestashop-project.org/8/webservice/resources/products/
    # NOTES : 
    ##############################
    
    def create_or_update_product(
        reference, nameFR, nameIT, price, quantity, active, descriptionFR, descriptionIT, link_rewriteFR, link_rewriteIT,
        meta_titleFR, meta_titleIT, meta_keywordsFR, meta_keywordsIT, meta_descriptionFR, meta_descriptionIT,
        categories, tax_rules_id, wholesale_price, on_sale, discount_amount, discount_percent, discount_from, discount_to,
        supplier_reference, supplier, manufacturer, ean13, upc, ecotax, width, height, depth, weight, 
        delivery_time_in_stock, delivery_time_out_of_stock, minimal_quantity, low_stock_level, receive_low_stock_alert,
        visibility, additional_shipping_cost, unity, unit_price, summaryFR, summaryIT, tags, url_rewrittenFR, url_rewrittenIT,
        text_when_in_stock, text_when_backorder_allowed, available_for_order, available_date, creation_date, show_price,
        image_urls, image_alt_texts, delete_existing_images, feature, available_online_only, condition, customizable,
        uploadable_files, text_fields, out_of_stock_action, virtual, file_url, allowed_downloads, expiration_date,
        number_of_days, shop_id, advanced_stock_management, depends_on_stock, warehouse, accessories):
        
        # Check if the product exists
        id = get_product_id_by_reference(reference)

        # Product XML data with all dynamic fields
        id_node = f"{'<id>'+str(id)+'</id>' if id else ''}"
        product_xml = f"""
        <prestashop xmlns:xlink="http://www.w3.org/1999/xlink">
            <product>
                {id_node}
                <active>{active}</active>
                <price>{price}</price>
                <quantity>{quantity}</quantity>
                <reference><![CDATA[{reference}]]></reference>
                <id_category_default>{categories.split(',')[0]}</id_category_default>
                <associations>
                    <categories>
                        {''.join([f'<category><id>{cat}</id></category>' for cat in categories.split(',')])}
                    </categories>
                    <accessories>
                        {''.join([f'<product><id>{acc}</id></product>' for acc in accessories.split(',')])}
                    </accessories>
                </associations>
                <tax_rules_group_id>{tax_rules_id}</tax_rules_group_id>
                <wholesale_price>{wholesale_price}</wholesale_price>
                <on_sale>{on_sale}</on_sale>
                <specific_prices>
                    <reduction>
                        <reduction_amount>{discount_amount}</reduction_amount>
                        <reduction_percent>{discount_percent}</reduction_percent>
                        <reduction_from>{discount_from}</reduction_from>
                        <reduction_to>{discount_to}</reduction_to>
                    </reduction>
                </specific_prices>
                <id_supplier>{supplier}</id_supplier>
                <id_manufacturer>{manufacturer}</id_manufacturer>
                <supplier_reference><![CDATA[{supplier_reference}]]></supplier_reference>
                <ean13><![CDATA[{ean13}]]></ean13>
                <upc><![CDATA[{upc}]]></upc>
                <ecotax>{ecotax}</ecotax>
                <width>{width}</width>
                <height>{height}</height>
                <depth>{depth}</depth>
                <weight>{weight}</weight>
                <delivery_in_stock><![CDATA[{delivery_time_in_stock}]]></delivery_in_stock>
                <delivery_out_stock><![CDATA[{delivery_time_out_of_stock}]]></delivery_out_stock>
                <minimal_quantity>{minimal_quantity}</minimal_quantity>
                <low_stock_threshold>{low_stock_level}</low_stock_threshold>
                <low_stock_alert>{receive_low_stock_alert}</low_stock_alert>
                <visibility><![CDATA[{visibility}]]></visibility>
                <additional_shipping_cost>{additional_shipping_cost}</additional_shipping_cost>
                <unity><![CDATA[{unity}]]></unity>
                <unit_price>{unit_price}</unit_price>
                <description_short>
                    <language id="1"><![CDATA[{summaryFR}]]></language>
                    <language id="2"><![CDATA[{summaryIT}]]></language>
                </description_short>
                <description>
                    <language id="1"><![CDATA[{descriptionFR}]]></language>
                    <language id="2"><![CDATA[{descriptionIT}]]></language>
                </description>
                <meta_title>
                    <language id="1"><![CDATA[{meta_titleFR}]]></language>
                    <language id="2"><![CDATA[{meta_titleIT}]]></language>
                </meta_title>
                <meta_keywords>
                    <language id="1"><![CDATA[{meta_keywordsFR}]]></language>
                    <language id="2"><![CDATA[{meta_keywordsIT}]]></language>
                </meta_keywords>
                <meta_description>
                    <language id="1"><![CDATA[{meta_descriptionFR}]]></language>
                    <language id="2"><![CDATA[{meta_descriptionIT}]]></language>
                </meta_description>
                <link_rewrite>
                    <language id="1"><![CDATA[{url_rewrittenFR}]]></language>
                    <language id="2"><![CDATA[{url_rewrittenIT}]]></language>
                </link_rewrite>
                <available_for_order>{available_for_order}</available_for_order>
                <available_date>{available_date}</available_date>
                <date_add>{creation_date}</date_add>
                <show_price>{show_price}</show_price>
                <images>
                    {''.join([f'<image><url><![CDATA[{url}]]></url><alt><![CDATA[{alt}]]></alt></image>' for url, alt in zip(image_urls.split(','), image_alt_texts.split(','))])}
                </images>
                <delete_existing_images>{delete_existing_images}</delete_existing_images>
                <features>
                    {''.join([f'<feature><name><![CDATA[{feat.split(":")[0]}]]></name><value><![CDATA[{feat.split(":")[1]}]]></value></feature>' for feat in feature.split(',')])}
                </features>
                <online_only>{available_online_only}</online_only>
                <condition><![CDATA[{condition}]]></condition>
                <customizable>{customizable}</customizable>
                <uploadable_files>{uploadable_files}</uploadable_files>
                <text_fields>{text_fields}</text_fields>
                <out_of_stock_action><![CDATA[{out_of_stock_action}]]></out_of_stock_action>
                <is_virtual>{virtual}</is_virtual>
                <file_url><![CDATA[{file_url}]]></file_url>
                <allowed_downloads>{allowed_downloads}</allowed_downloads>
                <expiration_date>{expiration_date}</expiration_date>
                <number_of_days>{number_of_days}</number_of_days>
                <id_shop_default>{shop_id}</id_shop_default>
                <advanced_stock_management>{advanced_stock_management}</advanced_stock_management>
                <depends_on_stock>{depends_on_stock}</depends_on_stock>
                <warehouse>
                    <id><![CDATA[{warehouse}]]></id>
                </warehouse>
            </product>
        </prestashop>
        """

        # Send POST or PATCH request based on existence
        if id:
            response = requests.patch(PRESTASHOP_API_URL + '/products' + "?output_format=JSON", data=product_xml, headers=prestashop_headers)
        else:
            response = requests.post(PRESTASHOP_API_URL + '/products' + "?output_format=JSON", data=product_xml, headers=prestashop_headers)

        if response.status_code in [200, 201]:
            logging.info(f"Product '{reference}' {'updated' if id else 'created'} successfully: {response.text}")
            return response.json().get('product', {}).get('id')
        else:
            logging.error(f"Failed to {'update' if id else 'create'} product '{reference}': {response.status_code}, {response.text}")
            return None

        if id:
            # Update the product
            prestashop_response = requests.patch(PRESTASHOP_API_URL + '/products' + "?output_format=JSON", data=product_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 200:
                logging.info(f"Product '{reference}' updated successfully: {prestashop_response.text}")
            else:
                logging.error(f"Failed to update product '{reference}': {prestashop_response.status_code}, {prestashop_response.text}")
        else:
            # Create the product
            logging.info(f"Creating the product '{reference}'")
            prestashop_response = requests.post(PRESTASHOP_API_URL + '/products' + "?output_format=JSON", data=product_xml, headers=prestashop_headers)
            if prestashop_response.status_code == 201:
                logging.info(f"Product '{reference}' created successfully: {prestashop_response.text}")
                # Grab the product ID
                id = prestashop_response.json()['product']['id']
            else:
                logging.error(f"Failed to create product '{reference}': {prestashop_response.status_code}, {prestashop_response.text}")
        return id
    
    ########################
    #   NOW, THE PROGRAM   #
    ########################
    
    ## STEP 1 - CREATE OR UPDATE THE MANUFACTURER (Marque en français)
    manufacturer_active = 1
    manufacturer_name = "Dynamic Manufacturer"
    manufacturer_descriptionFR = "Description en français."
    manufacturer_descriptionIT = "Descrizione in italiano."
    manufacturer_short_descriptionFR = "Short description of the manufacturer."
    manufacturer_short_descriptionIT = "Short description of the manufacturer."
    manufacturer_meta_titleFR = "Dynamic Manufacturer Meta Title"
    manufacturer_meta_titleIT = "Dynamic Manufacturer Meta Title"
    manufacturer_meta_keywordsFR = "dynamic,manufacturer,prestashop"
    manufacturer_meta_keywordsIT = "dynamic,manufacturer,prestashop"
    manufacturer_meta_descriptionFR = "Meta description for the dynamic manufacturer."
    manufacturer_meta_descriptionIT = "Meta description for the dynamic manufacturer."
    manufacturer_logo_url = "https://picsum.photos/200"
    manufacturer_id = create_or_update_manufacturer(manufacturer_name,manufacturer_active,manufacturer_descriptionFR,manufacturer_descriptionIT,manufacturer_short_descriptionFR,manufacturer_short_descriptionIT,manufacturer_meta_titleFR,manufacturer_meta_titleIT,manufacturer_meta_keywordsFR,manufacturer_meta_keywordsIT,manufacturer_meta_descriptionFR,manufacturer_meta_descriptionIT)
    set_manufacturer_logo(manufacturer_id,manufacturer_logo_url)
    
    
    ## STEP 2 - CREATE OR UPDATE THE SUPPLIER
    # SUPPLIER DATA
    supplier_active = 1
    supplier_name = "Dynamic Supplier 2"
    supplier_descriptionFR = "This is a detailed description of the supplier in french."
    supplier_descriptionIT = "This is a detailed description of the supplier in italian."
    supplier_meta_titleFR = "Dynamic Supplier Meta Title"
    supplier_meta_titleIT = "Dynamic Supplier Meta Title"
    supplier_meta_keywordsFR = "dynamic,supplier,prestashop"
    supplier_meta_keywordsIT = "dynamic,supplier,prestashop"
    supplier_meta_descriptionFR = "Meta description for the dynamic supplier."
    supplier_meta_descriptionIT = "Meta description for the dynamic supplier."
    # SUPPLIER ADDRESS DATA
    supplier_address_active = 1
    supplier_address_alias = supplier_name + " Address Alias"
    supplier_address_company = supplier_name
    supplier_address_firstname = "Contact Firstname"
    supplier_address_lastname = "Contact Lastname"
    supplier_address_address1 = "123 VIA STREET"
    supplier_address_address2 = "Suite 45678"
    supplier_address_postcode = "12345"
    supplier_address_city = "Supplier City"
    supplier_address_other = ""
    supplier_address_country = "8" # Country ID (see table ps_country : 1 DE, 6 ES, 7 GB, 8 FR, 10 IT)
    supplier_address_state = "" # State ID (see table ps_state) for countries with states such as CA, IT, JP, US, AU, AR, IN, ID, MX
    supplier_address_phone = "+1234567890"
    supplier_address_phone_mobile = "+1234567890"
    supplier_address_dni = "" # ID number (number on National ID Card or passport)
    # SUPPLIER LOGO
    supplier_logo_url = "https://picsum.photos/200"
    supplier_id = create_or_update_supplier(supplier_name,supplier_active,supplier_descriptionFR,supplier_descriptionIT,supplier_meta_titleFR,supplier_meta_titleIT,supplier_meta_keywordsFR,supplier_meta_keywordsIT,supplier_meta_descriptionFR,supplier_meta_descriptionIT)
    supplier_address_id = create_or_update_supplier_address(supplier_id,supplier_address_active,supplier_address_alias,supplier_address_company,supplier_address_firstname,supplier_address_lastname,supplier_address_address1,supplier_address_address2,supplier_address_postcode,supplier_address_city,supplier_address_other,supplier_address_country,supplier_address_state,supplier_address_phone,supplier_address_phone_mobile,supplier_address_dni)
    set_supplier_logo(supplier_id, supplier_logo_url)


    # STEP 3 - CREATE THE CATEGORY
    category_parent_category = "2"  # ID of the parent category (see table ps_category and ps_category_lang : 1 for Root/Racine, 2 for Home/Accueil)
    category_nameFR = "Protection respiratoire"
    category_nameIT = "Protezione respiratoria"
    category_active = 1  # 1 for active, 0 for inactive
    #category_is_root_category = 0  # 1 for root, 0 for non-root
    #category_id_shop_default = 1  # ID of the shop (see table ps_shop : 1 for Bleuagro)
    #category_position = 0  # position of the category
    category_descriptionFR = "This is a description of the category in French. Modified."
    category_descriptionIT = "This is a description of the category iun Italian. Modificato."
    #category_additional_descriptionFR = "This is more information about the category in French."
    #category_additional_descriptionIT = "This is more information about the category in Italian."
    category_meta_titleFR = "Dynamic Category Meta Title"
    category_meta_titleIT = "Dynamic Category Meta Title"
    category_meta_keywordsFR = "dynamic,category,prestashop"
    category_meta_keywordsIT = "dynamic,category,prestashop"
    category_meta_descriptionFR = "Meta description for the dynamic category."
    category_meta_descriptionIT = "Meta description for the dynamic category."
    category_link_rewriteFR = urlify(category_nameFR) #simplified url for this category
    category_link_rewriteIT = urlify(category_nameIT) #simplified url for this category
    category_image_url = "https://picsum.photos/200"
    category_id = create_or_update_category(category_parent_category,category_nameFR,category_nameIT,category_active,category_descriptionFR,category_descriptionIT,category_link_rewriteFR,category_link_rewriteIT,category_meta_titleFR,category_meta_titleIT,category_meta_keywordsFR,category_meta_keywordsIT,category_meta_descriptionFR,category_meta_descriptionIT)
    set_category_logo(category_id, category_image_url)
        
    # STEP 4 - CREATE THE PRODUCT
    product_reference = "REF123456"
    product_active = 1  # 1 for active, 0 for inactive
    product_nameFR = "Dynamic Product FR"
    product_nameIT = "Dynamic Product IT"
    product_price_tax_excluded = 19.99
    product_quantity = 100
    product_descriptionFR = "This is a detailed product description in french."
    product_descriptionIT = "This is a detailed product description in italian."
    product_link_rewriteFR = urlify(product_nameFR) #simplified url for this category
    product_link_rewriteIT = urlify(product_nameIT) #simplified url for this category
    product_meta_title = "Dynamic Product Meta Title"
    product_meta_keywords = "keyword1,keyword2"
    product_meta_description = "This is the meta description."
    product_meta_titleFR = "Dynamic Product Meta Title"
    product_meta_titleIT = "Dynamic Product Meta Title"
    product_meta_keywordsFR = "dynamic,product,prestashop"
    product_meta_keywordsIT = "dynamic,product,prestashop"
    product_meta_descriptionFR = "Meta description for the dynamic product."
    product_meta_descriptionIT = "Meta description for the dynamic product."
    product_categories = "2,3,4"  # Categories by ID (x,y,z,...)
    product_tax_rules_id = 1
    product_wholesale_price = 10.00
    product_on_sale = 1  # 1 for on sale, 0 for not
    product_discount_amount = 5.00
    product_discount_percent = 10
    product_discount_from = "2024-01-01"
    product_discount_to = "2024-12-31"
    product_supplier_reference = "SUPP123"
    product_supplier = "1"  # Supplier ID
    product_manufacturer = "1"  # Manufacturer ID
    product_ean13 = "1234567890123"
    product_upc = "123456789012"
    product_ecotax = 0.10
    product_width = 10.00
    product_height = 20.00
    product_depth = 5.00
    product_weight = 0.5
    product_delivery_time_in_stock = "3-5 days"
    product_delivery_time_out_of_stock = "7-10 days"
    product_minimal_quantity = 1
    product_low_stock_level = 10
    product_receive_low_stock_alert = 1  # 1 for Yes, 0 for No
    product_visibility = "both"  # "both", "catalog", "search"
    product_additional_shipping_cost = 0.00
    product_unity = "1"
    product_unit_price = 19.99
    product_summaryFR = "This is a summary of the product in FR."
    product_summaryIT = "This is a summary of the product in IT."
    product_tags = "tag1,tag2,tag3"
    product_url_rewrittenFR = "dynamic-product-fr"
    product_url_rewrittenIT = "dynamic-product-it"
    product_text_when_in_stock = "In Stock"
    product_text_when_backorder_allowed = "Available for backorder"
    product_available_for_order = 1  # 0 = No, 1 = Yes
    product_available_date = "2024-12-03"
    product_creation_date = "2024-12-03"
    product_show_price = 1  # 0 = No, 1 = Yes
    product_image_urls = "http://example.com/image1.jpg,http://example.com/image2.jpg"
    product_image_alt_texts = "Image 1 Alt, Image 2 Alt"
    product_delete_existing_images = 0  # 0 = No, 1 = Yes
    product_feature = "Color:Red:1,Size:L:2"
    product_available_online_only = 0  # 0 = No, 1 = Yes
    product_condition = "new"  # "new", "used", "refurbished"
    product_customizable = 0  # 0 = No, 1 = Yes
    product_uploadable_files = 0  # 0 = No, 1 = Yes
    product_text_fields = 0  # 0 = No, 1 = Yes
    product_out_of_stock_action = "waiting"  # "deny", "allow", "waiting"
    product_virtual = 0  # 0 = No, 1 = Yes
    product_file_url = "http://example.com/file"
    product_allowed_downloads = 5
    product_expiration_date = "2025-12-31"
    product_number_of_days = 365
    product_shop_id = "1"  # Shop ID
    product_advanced_stock_management = 1  # 1 for enabled, 0 for disabled
    product_depends_on_stock = 1  # 1 for enabled, 0 for disabled
    product_warehouse = "1"  # Warehouse ID
    product_accessories = "10,11,12"  # Accessory product IDs
    create_or_update_product(
        product_reference, product_nameFR, product_nameIT, product_price_tax_excluded, product_quantity, product_active, product_descriptionFR, product_descriptionIT, product_link_rewriteFR, product_link_rewriteIT,
        product_meta_titleFR, product_meta_titleIT, product_meta_keywordsFR, product_meta_keywordsIT, product_meta_descriptionFR, product_meta_descriptionIT,
        product_categories, product_tax_rules_id, product_wholesale_price, product_on_sale, product_discount_amount, product_discount_percent, product_discount_from, product_discount_to,
        product_supplier_reference, product_supplier, product_manufacturer, product_ean13, product_upc, product_ecotax, product_width, product_height, product_depth, product_weight,
        product_delivery_time_in_stock, product_delivery_time_out_of_stock, product_minimal_quantity, product_low_stock_level, product_receive_low_stock_alert,
        product_visibility, product_additional_shipping_cost, product_unity, product_unit_price, product_summaryFR, product_summaryIT, product_tags, product_url_rewrittenFR, product_url_rewrittenIT,
        product_text_when_in_stock, product_text_when_backorder_allowed, product_available_for_order, product_available_date, product_creation_date, product_show_price,
        product_image_urls, product_image_alt_texts, product_delete_existing_images, product_feature, product_available_online_only, product_condition, product_customizable,
        product_uploadable_files, product_text_fields, product_out_of_stock_action, product_virtual, product_file_url, product_allowed_downloads, product_expiration_date,
        product_number_of_days, product_shop_id, product_advanced_stock_management, product_depends_on_stock, product_warehouse, product_accessories)
       
   ## STEP 5 - UPDATE AVAILABLE QUANTITIES USING A PATCH REQUEST
   #
   ## Function to get the product ID by product reference
   #product_reference = "TST001"  # Product reference to search for
   #new_quantity = 50  # New quantity to set
   #def get_product_id_by_reference(reference):
   #    prestashop_response = requests.get(PRESTASHOP_API_URL + '/products?filter[reference]=' + reference + "&output_format=JSON", headers=prestashop_headers)
   #    if prestashop_response.status_code == 200:
   #        products = prestashop_response.json().get('products', [])
   #        if products:
   #            return products[0]['id']
   #        else:
   #            logging.info("Product '" + reference + "' not found.")
   #            return None
   #    else:
   #        logging.info(f"Failed to fetch product '{reference}' by reference: {prestashop_response.status_code}, {prestashop_response.text}")
   #        return None
   #
   #product_id = get_product_id_by_reference(product_reference)
   #if product_id:
   #    # XML to update only the quantity for the product
   #    quantity_xml = f"""
   #    <prestashop xmlns:xlink="http://www.w3.org/1999/xlink">
   #        <product>
   #            <id>{product_id}</id>
   #            <associations>
   #                <stock_availables>
   #                    <stock_available>
   #                        <id_product>{product_id}</id_product>
   #                        <quantity>{new_quantity}</quantity>
   #                    </stock_available>
   #                </stock_availables>
   #            </associations>
   #        </product>
   #    </prestashop>
   #    """
   #    
   #    prestashop_response = requests.patch(PRESTASHOP_API_URL + '/stock_availables/' + product_id + "?output_format=JSON", data=quantity_xml, headers=prestashop_headers)
   #    # Log the response
   #    if prestashop_response.status_code == 200:
   #        logging.info(f"Stock updated successfully: {prestashop_response.text}")
   #    else:
   #        logging.info(f"Failed to update stock: {prestashop_response.status_code}, {prestashop_response.text}")
   #        prestashop_response.raise_for_status()
    
   ## STEP 6 - CREATE COMBINATIONS
   ## STEP 7 - CREATE THE CUSTOMER AND ITS ADDRESSES

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
