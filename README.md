# 12-132023

# 12-12-2023

import requests
import json
import pandas as pd
from datetime import date
from datetime import timezone, timedelta, datetime, date
from sqlalchemy import create_engine
from bs4 import BeautifulSoup

class naver_review_api():
    def __init__(self,brand: str) :
        self.brand = brand
        self.today = datetime.today()
        self.reviews = pd.DataFrame()

        if brand == 'Aldicom':
            wholeStoreCategoryId = '40e252c16afb4d02b369ce59505676b8'
            self.find_url = 'https://m.smartstore.naver.com/aldicom/category/'+wholeStoreCategoryId

        if brand == 'Cooltia':
            wholeStoreCategoryId = '5eb0a46bdd47469abdc2641d8c9053df'
            self.find_url = 'https://m.smartstore.naver.com/jorboka12/category/'+wholeStoreCategoryId


    def find_key_and_return_value(self, dictionary, target_key):
        for key, value in dictionary.items():
            if key == target_key:
                return value
            elif isinstance(value, dict):
                result = self.find_key_and_return_value(value, target_key)
                if result is not None:
                    return result
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        result = self.find_key_and_return_value(item, target_key)
                        if result is not None:
                            return result

    def extract_product_nos(self, data):
        product_nos = []
        def extract_values(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key == 'productNo':
                        if value is not None or value == 0:
                            product_nos.append(value)
                    elif isinstance(value, (dict, list)):
                        extract_values(value)
            elif isinstance(obj, list):
                for item in obj:
                    extract_values(item)

        extract_values(data)
        return product_nos

    def query_db(self, sql):
        engine = create_engine('postgresql+psycopg2://wholesum:John4499!@wholesum.postgres.database.azure.com/postgres',
                            connect_args={'sslmode': 'require'})
        #engine = connect_db()
        df = pd.read_sql(sql, engine)
        return df

    def insert_to_sql(self, df, table_name):
        engine = create_engine('postgresql+psycopg2://wholesum:John4499!@wholesum.postgres.database.azure.com/postgres')
        df.columns = df.columns.map(lambda x : x.lower())
        try:
            df.to_sql(table_name, con=engine, index=False, if_exists='append')
        except Exception as e:
            print(e)
            pass

    def find_api_meterials(self):
        payload = {'cp': 1}
        response = requests.get(self.find_url, params=payload)
        if response.status_code == 200:
            print("Request was successful. Status Code:", response.status_code)
            page_content = response.text
            to_json = page_content.find('window.__PRELOADED_STATE__')
            to_json = page_content[to_json+27:]
            to_json = to_json.split('</script>')[0]
            data = json.loads(to_json)

            self.checkoutMerchantNo = self.find_key_and_return_value(data, 'payReferenceKey')
            self.originProductNo = self.extract_product_nos(data)

            print('checkoutMerchantNo: ',self.checkoutMerchantNo)
            print('originProductNo: ',self.originProductNo)
        else:
            print("Failed to fetch data. Status Code:", response.status_code)

    def  api_read(self):
        url = 'https://m.smartstore.naver.com/i/v1/contents/reviews/query-pages'
        headers = {'Content-Type': 'application/json;charset=UTF-8'}

        for num in self.originProductNo:
            payload = {"checkoutMerchantNo": self.checkoutMerchantNo, "originProductNo": num}
            response = requests.post(url, headers=headers, data = json.dumps(payload))

            if response.status_code == 204:
                print(f"{num} Request was successful. No content returned.")
                pass
            elif response.status_code == 200:
                print(f"{num} Request was successful. Content returned.")
            else:
                print(f"{num} Failed to fetch data. Status Code:", response.status_code)

            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"{num} JSONDecodeError: Failed to parse JSON response.")
                continue  # Skip processing if JSON parsing fails

            #data = response.json()
            print(num, data.get('totalElements'), data.get('totalPages'), len(data.get('contents')))

            page_num = data.get('totalPages')
            review=pd.DataFrame()
            self.reviews = pd.DataFrame()
            for p in range(1,page_num+1):
                payload = {
                    "checkoutMerchantNo": self.checkoutMerchantNo,
                    "originProductNo": num,
                    "page": p,
                    "pageSize": 20,
                    "reviewSearchSortType": "REVIEW_CREATE_DATE_DESC"
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))

                try:
                    data = response.json()
                except json.JSONDecodeError:
                    print(f"{num} JSONDecodeError: Failed to parse JSON response for page {p}.")
                    break
                    continue  # Skip processing this page if JSON parsing fails

                binary_content = response.content.decode('utf-8', 'ignore')
                temp = pd.DataFrame.from_records(json.loads(binary_content).get('contents'))
                # Now, create a DataFrame from the cleaned content
                #temp = pd.DataFrame.from_records(data.get('contents'))
                
                temp['brand'] = 'Cooltia'
                temp['channel'] = 'Naver'
                temp['date'] = datetime.today()

                review = pd.concat([review, temp])

                print(f"{num} Page {p}: Total rows {len(review)}")
            self.reviews = pd.concat([self.reviews, review])
            print(len(self.reviews.value_counts('id')))
            #print(f"{num} Page {p}: Total rows {len(self.reviews)}")

    def validate_new(self):
        existing = self.query_db(f'''
                        select distinct(review_id)
                        from reviews_naver
                        where brand = '{self.brand}'
        ''')
        print('# of exist ttl:', len(existing), '# of ttl today:', len(self.reviews))

        existing_review_ids = existing['review_id'].astype(float).tolist()
        self.new_reviews = self.reviews[~self.reviews['reviewId'].astype(float).isin(existing_review_ids)]
        

        if len(self.new_reviews) == 0:
            print('Not any new reviews')
        else:
            print(len(self.new_reviews))
            self.processing()


    def data_preprocessing(self):
        df = self.reviews

        if 'productOptionContent' in df.columns:
            df = df[['brand', 'channel', 'productNo', 'createDate', 'id', 'writerId', 'productName','productOptionContent', 'reviewScore', 'reviewType','repurchase', 'reviewContent', 'date']]
            df.columns = ['brand', 'channel', 'product_code', 'review_date', 'review_id', 'user', 'product_name', 'product_option', 'rating', 'review_type', 'repurchase', 'review_details', 'date']
        else:
            df = df[['brand', 'channel', 'productNo', 'createDate', 'id', 'writerId', 'productName','reviewScore', 'reviewType','repurchase', 'reviewContent', 'date']]
            df.columns = ['brand', 'channel', 'product_code', 'review_date', 'review_id', 'user', 'product_name', 'rating', 'review_type', 'repurchase', 'review_details', 'date']
        
        df = df.sort_values(by='review_date', ascending=False).reset_index(drop=True)
        df = df.applymap(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
        self.insert_to_sql(df, 'reviews_naver')

