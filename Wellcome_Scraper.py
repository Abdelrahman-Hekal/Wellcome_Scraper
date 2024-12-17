import time 
import os
from datetime import datetime
import pandas as pd
import sys
import sys
import shutil
import requests
import warnings
import pymongo
warnings.filterwarnings('ignore')

def get_inputs():
    print('Processing The Settings Sheet ...')
    # assuming the inputs to be in the same script directory
    path = os.path.join(os.getcwd(), 'settings.xlsx')
    if not os.path.isfile(path):
        print('Error: Missing the settings file "settings.xlsx"')
        input('Press any key to exit')
        sys.exit(1)
    try:
        links = []
        df = pd.read_excel(path)
        cols  = df.columns
        for col in cols:
            df[col] = df[col].astype(str)

        inds = df.index
        for ind in inds:
            row = df.iloc[ind]
            for col in cols:
                if row[col] == 'nan': continue
                elif col == 'URL':
                    links.append(row[col])                
    except:
        print('Error: Failed to process the settings sheet')
        input('Press any key to exit')
        sys.exit(1)

    return links

def initialize_output():
    stamp = datetime.now().strftime("%d_%m_%Y_%H_%M")
    file = f'Wellcome_{stamp}.xlsx'
    path = os.path.join(os.getcwd(), 'scraped_data', stamp)

    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)

    output = os.path.join(path, file)
    return output

def scrape_products(links, output1):
    df = pd.DataFrame()
    searchUrl = "https://searchgw.rta-os.com/app/search/wareSearch"
    categoryUrl = "https://searchgw.rta-os.com/app/wareCategory/list"
    prodDetailsUrl = "https://detail.rta-os.com/app/wareDetail/baseinfo"
    headers = {
        "dmTenantId": "15",
        "User-Agent": "dmall/6.13.0 Dalvik/2.1.0 (Linux; U; Android 11; sdk_gphone_x86 Build/RSR1.240422.006)",
        "dmall-locale": "en_US",
        "apiVersion": "6.13.0",
        "param-class": "java.lang.String",
        "appName": "com.rtahk.wellcome",
        "env": "app",
        "version": "6.13.0",
        "venderStoreIds": "5-43,5-317",
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "searchgw.rta-os.com",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip",
    }
    searchPayload ='param=%7B%22brandId%22%3A%22%22%2C%22businessCode%22%3A1%2C%22categoryId%22%3A%22{catId}%22%2C%22categoryLevel%22%3A1%2C%22categorySkuId%22%3A0%2C%22categoryType%22%3A1%2C%22erpStoreId%22%3A%22%22%2C%22filterProperties%22%3A%5B%5D%2C%22from%22%3A1%2C%22globalSelection%22%3Afalse%2C%22noResultSearch%22%3A0%2C%22pos%22%3A1%2C%22proTagId%22%3A0%2C%22promoting%22%3A0%2C%22sortKey%22%3A0%2C%22sortRule%22%3A0%2C%22src%22%3A0%2C%22venderId%22%3A%22%22%2C%22pageNum%22%3A%22{page}%22%2C%22pageSize%22%3A%2220%22%7D'
    prodPayload = "param=%7B%22lat%22%3A22.2847577%2C%22lng%22%3A114.1326485%2C%22sku%22%3A%22{sku}%22%2C%22skuNum%22%3A0%7D"
    for url in links:     
        catId = url.split("/")[-2]
        # Getting the category details
        for _ in range(10):
            try:
                response = requests.post(categoryUrl, headers=headers, data={})
                if response.status_code == 200:
                    break
            except:
                pass

        catData = response.json()
        categories = catData["data"]["wareCategory"][0]["categoryList"]
        for cat in categories:
            if cat["categoryId"] == catId:
                catDetails = cat
                break

        # Getting the brand and Dietary Needs keywords for the category
        catPayload = searchPayload.replace('%22{catId}%22', f'%22{catId}%22').replace('%22{page}%22', f'%221%22')
        for _ in range(10):
            try:
                response = requests.post(searchUrl, headers=headers, data=catPayload)
                if response.status_code == 200:
                    catData = response.json()
                    if "data" in catData:
                        break
                    else:
                        time.sleep(5)
            except:
                pass
        brands, dietaryNeeds = [], []
        for property in catData["data"]["properties"]:
            if property["propertyName"] == "Brands":
                for brand in property["childProperties"]:
                    brands.append(brand["propertyName"].lower())
            elif property["propertyName"] == "Dietary Needs":
                for need in property["childProperties"]:
                    dietaryNeeds.append(need["propertyName"].lower())

        categoryName = catDetails["categoryName"]
        for subcategory in catDetails["childCategoryList"]:
            subcategoryName = subcategory["categoryName"]
            subcategoryId = subcategory["categoryId"]
            for group in subcategory["childCategoryList"]:
                groupName = group["categoryName"]
                groupId = group["categoryId"]

                groupPayload = searchPayload.replace('%22{catId}%22', f'%22{groupId}%22').replace('%22{page}%22', f'%221%22')

                for _ in range(10):
                    try:
                        response = requests.post(searchUrl, headers=headers, data=groupPayload)
                        if response.status_code == 200:
                            groupData = response.json()
                            if "data" in groupData:
                                break
                            else:
                                time.sleep(5)
                    except:
                        pass
                    
                nprods = groupData["data"]["pageInfo"]["total"]
                print("-"*50)
                print(f"Crawling Group: {groupName}")
                print("-"*50)
                print(f"Number of products: {nprods}")
                npages = groupData["data"]["pageInfo"]["pageCount"]

                for page in range(1, npages+1):
                    print(f"crawling page: {page}/{npages}")
                    groupPayload = searchPayload.replace('%22{catId}%22', f'%22{groupId}%22').replace('%22{page}%22', f'%22{page}%22')
                    for _ in range(10):
                        try:
                            response = requests.post(searchUrl, headers=headers, data=groupPayload)
                            if response.status_code == 200:
                                groupData = response.json()
                                if "data" in groupData:
                                    break
                                else:
                                    time.sleep(5)
                        except:
                            pass

                    # Crawling group products
                    prods = groupData["data"]["wareList"]
                    for prod in prods:
                        sku = prod["sku"]
                        payload = prodPayload.replace("%22{sku}%22", f"%22{sku}%22")
                        prodHeaders = headers.copy()
                        prodHeaders["Host"] = "detail.rta-os.com"
                        prodHeaders["storeId"] = str(prod["storeId"])
                        prodHeaders.pop("venderStoreIds")
                        for _ in range(10):
                            try:
                                response = requests.post(prodDetailsUrl, headers=prodHeaders, data=payload)
                                if response.status_code == 200:
                                    prodData = response.json()
                                    if "data" in prodData:
                                        break
                                    else:
                                        time.sleep(5)
                            except:
                                pass
                        prodDetails = prodData["data"]
                        row = {}
                        row["name"] = prod["wareName"]
                        row["sku"] = prod["sku"]
                        if row["sku"] == 101359285:
                            debug = True
                        try:
                            row["original_price"] = prod["onlinePrice"] / 100
                        except:
                            pass
                        try:
                            row["retail_price"] = prod["onlinePromotionPrice"] / 100
                        except:
                            pass
                        row["category"] = categoryName
                        row["sub_category"] = subcategoryName
                        row["group"] = groupName
                        try:
                            row["spec"] = prodDetails["packingSpecification"]
                        except:
                            pass
                        try:
                            row["origin"] = prodDetails["produceArea"]
                        except:
                            pass
                        try:
                            imgs = []
                            for elem in prodDetails["wareImgListNew"]:
                                if "url" in elem:
                                    imgs.append(elem["url"])
                            row["product_image_urls"] = imgs
                        except:
                            pass
                        try:
                            row["storage_method"] = prodDetails["storageTypeName"]
                        except:
                            pass
                        try:
                            delivery = []
                            if "Deliver to any defined address" in prodDetails["deliveryDesc"]:
                                delivery.append("Home Delivery")
                            if prodDetails["allowCc"] == 1:
                                delivery.append("Click & Collect")
                            row["delivery_methods"] = delivery
                        except:
                            pass
                        try:
                            tags = []
                            for tag in prodDetails["promotionWareVO"]["promotionInfoList"]:
                                tags.append(tag["displayInfo"]["proTag"])
                            row["offer_tags"] = tags
                        except:
                            pass
                        try:
                            for brand in brands:
                                if brand in row["name"].lower():
                                    row["brand"] = brand.title()
                                    break
                            # for need in dietaryNeeds:
                            #     if need in row["name"].lower():
                            #         row["dietary_need"] = need.title()
                            #         break
                        except:
                            pass
                        try:
                            if prodDetails["wareStock"] == 0:
                                row["sold_out"] = True
                            else:
                                row["sold_out"] = False
                            row["stock"] = prodDetails["wareStock"]
                        except:
                            pass
                        try:
                            row["product_url"] = "https://www.wellcome.com.hk/en/p/" + prod["wareName"] + "/i/" + str(prod["sku"]) + ".html"
                            row["product_url"] = row["product_url"].replace(" ", "%20")
                        except:
                            pass
                        df = pd.concat([df, pd.DataFrame([row.copy()])], ignore_index=True)

    if df.shape[0] > 0:
        df['extraction_date'] = datetime.now()
        # Convert 'extraction_date' to a string or datetime.datetime for MongoDB compatibility
        df['extraction_date'] = df['extraction_date'].apply(lambda x: pd.Timestamp(x).to_pydatetime())
        df.to_excel(output1, index=False)  

              
if __name__ == '__main__':
    stamp = datetime.now().strftime("%m/%d/%Y")
    start = time.time()
    links = get_inputs()
    output = initialize_output()
    for _ in range(5):
        try:
            print('-'*75)
            print('Scraping products ...')
            scrape_products(links, output)
            break
        except Exception as err: 
            print(f'Error: {err}')

    print('-'*75)
    time_mins = round(((time.time() - start)/60), 2)
    hrs = round(time_mins/60, 2)
    input(f'Process is completed successfully in {time_mins} mins ({hrs} hours). Press any key to exit.')