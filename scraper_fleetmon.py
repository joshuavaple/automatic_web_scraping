
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime
import io

def get_current_timestamp():
    """
    Returns a string of current timestamp in YYYYmmdd_HHMMSS format
    """
    now = datetime.datetime.now()
    return now.strftime("%Y%m%d_%H%M%S")

def write_dataframe_to_azure_blob(dataframe, connection_string, container_name, file_name, file_extension):
    """
    Directly writes an in-memory dataframe to azure blob storage container
    """
    # Convert DataFrame to CSV or Excel file in memory
    file_name_full = file_name + file_extension
    if file_extension == '.csv':
        file_content = dataframe.to_csv(index=False)
        content_type = 'text/csv'
    elif file_extension == '.xlsx':
        with io.BytesIO() as excel_buffer:
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                dataframe.to_excel(writer, index=False)
            excel_buffer.seek(0)
            file_content = excel_buffer.read()
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif file_extension == '.json':
        file_content = dataframe.to_json(orient='split')
        content_type='application/json'
    else:
        raise ValueError("File format not supported. Please provide either CSV, Excel or JSON file format ending.")

    # Create BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # Upload the file to blob storage
    try: 
        blob_file_name = get_current_timestamp() + '_' + file_name_full
        blob_client = container_client.get_blob_client(blob_file_name)
        blob_client.upload_blob(file_content, blob_type="BlockBlob", content_type=content_type)
        print(f"Successfully uploaded '{file_name}' to Azure Blob Storage.")
    except Exception as e:
        print(f"An error occurred while uploading '{file_name}': {str(e)}")

def get_article_urls(parent_page_urls: list):
    article_urls = []
    domain = 'https://www.fleetmon.com'
    for url in parent_page_urls:
        # Send a GET request to the URL
        response = requests.get(url, verify=False)
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find all anchor tags (i.e., <a> tags) in the HTML content
        for link in soup.find_all('a', {'class': 'continue-reading'}):
            article_url = domain + link.get('href')
            article_urls.append(article_url)
    return article_urls

def get_article_texts(article_urls: list):
    results = {'title':[], 'body':[], 'url':[]}
    for url in article_urls:
        results['url'].append(url)
        response = requests.get(url, verify = False)
        # Use BeautifulSoup to parse the HTML content of the response
        soup = BeautifulSoup(response.content, 'html.parser')
        h1_tags = soup.find_all('h1')
        header_text = ''
        for h1_tag in h1_tags:
            header_text += h1_tag.get_text()
        results['title'].append(header_text)
        # Find all the <p> tags and extract the longest text, which is the article body most of the time
        p_tags = soup.find_all('p')
        article_texts = []
        for p_tag in p_tags:
            article_texts.append(p_tag.get_text())
        results['body'].append(max(article_texts, key=len))
        df_articles = pd.DataFrame(results)
    return df_articles


def run():
    file_name="fleetmon_scraping_results"
    max_page = 3
    connection_string = "DefaultEndpointsProtocol=https;AccountName=joshualestorage;AccountKey=/JsrSWHnwBah6WzNaZKY1piSVFyK2i+eWvY2lAMVON9w/kIi8xf4LxDh+d+SQhiFgMJ7VZmmuMk7+ASta99p+w==;EndpointSuffix=core.windows.net"
    container_name = 'webscraping'
    output_format = '.xlsx'

    parent_page_urls = ['https://www.fleetmon.com/maritime-news/?page=' + str(page) + '&category=incidents' for page in range(1, max_page + 1)]
    article_urls = get_article_urls(parent_page_urls=parent_page_urls)
    df_articles = get_article_texts(article_urls=article_urls)
    write_dataframe_to_azure_blob(dataframe = df_articles,
                                connection_string=connection_string,
                                container_name=container_name,
                                file_name=file_name,
                                file_extension=output_format)
if __name__ == '__main__':
    run()