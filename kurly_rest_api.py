"""
Kurly의 API를 활용하여 데이터 수집
"""

import re
import json
import asyncio
from datetime import datetime, timedelta

import aiohttp
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class ChromeDriver:
    """
    Chrome Driver를 통해 Selenium을 활용하는 Class

    REST API를 활용하여 데이터를 수집하는 방법에서는 Selenium은 access_token을 얻기 위한 용도로만 사용
    """
    def __init__(self, url: str) -> None:
        self.url = url

    def get_logs(self) -> list:
        options = webdriver.ChromeOptions()  # 크롬 옵션 객체 생성
        options.add_argument("headless")  # headless 모드 설정(display 모드를 사용하지 않음)

        capabilities = DesiredCapabilities.CHROME
        #REST API를 사용할 때 필요한 access_token 값을 추출하기 위해 브라우저의 모든 performance 로그 데이터를 수집
        capabilities["goog:loggingPrefs"] = {"performance": "ALL"}  
        capabilities["goog:pageLoadStrategy"] = "none"
        
        service = Service("chromedriver")

        driver = webdriver.Chrome(
            service=service,
            options=options,
            desired_capabilities=capabilities,
        )
        driver.get(self.url)
        return driver.get_log("performance")

    def get_bearer_token(self) -> str:
        """
        Kurly REST API에 접근할 수 있는 access_token을 return하는 메서드
        """
        logs = self.get_logs()
        for entry in logs:
            log = json.loads(entry["message"])["message"]
            if "request" in log["params"].keys() and log["params"]["request"]["headers"].get("Authorization"):
                return log["params"]["request"]["headers"].get("Authorization")

class KurlyClient:
    """
    API를 활용하여 Kurly 홈페이지에 있는 데이터를 호출하는 class

    :get_best_items
    :Kurly의 베스트 상품을 조회할 수 있는 메서드

    :get_review_count
    :상품 당 리뷰 개수를 조회할 수 있는 메서드

    :get_reviews
    :상품의 리뷰를 조회할 수 있는 메서드
    """
    def __init__(self, token: str) -> None:
        self.token = token
        self.headers = self.get_headers(token)

    def get_headers(self, token: str):
        headers = {
        'authority': 'api.kurly.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'authorization': token,
        'origin': 'https://www.kurly.com',
        'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
        }
        return headers

    async def get_best_items(self, per_page: int = 50):
        """
        :per_page : str
        :상품 개수를 뜻하는 파라미터로 수집하고 싶은 베스트 상품 개수를 입력하면 됨(기본 값으로 50개의 상품을 조회하도록 설정)
        """
        url = f"https://api.kurly.com/collection/v2/home/product-collections/market-best/products?sort_type=4&page=1&per_page={per_page}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=self.headers) as resp:
                return await resp.json()

    async def get_review_count(self, product_no: int) -> int:
        url = f"https://api.kurly.com/product-review/v1/contents-products/{product_no}/count"
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=self.headers) as resp:
                try:
                    data = await resp.json()
                    return data["data"]["count"]
                except:
                    print(await resp.json())
                
    async def get_reviews(self, product_no: int, size: int = None, sort_type: str = "RECENTLY"):
        """
        :size : int
        :한번에 호출하고 싶은 리뷰 개수를 입력하면 됨. 별도로 입력하지 않을 경우 해당 상품의 전체 리뷰를 호출함 \n
         (전체 리뷰를 호출할 경우 IP가 일시 차단될 수 있으니 주의)

        :sort_type : str
        :리뷰 조회시 필터 조건을 '최신순', '추천순'을 선택하여 조회할 수 있는 파라미터. 기본 값으로는 '최신순'으로 수집하도록 설정
        """
        if size is None:
            size = await self.get_review_count(product_no)
        url = f"https://api.kurly.com/product-review/v1/contents-products/{product_no}/reviews?sortType={sort_type}&size={size}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=self.headers) as resp:
                return await resp.json()

class DataParse:
    """
    Kurly API를 활용하여 수집한 데이터를 엑셀 양식에 맞게 가공하는 class

    :item_parse
    :상품 정보를 가공하는 메서드

    :review_parse
    :리뷰 정보를 가공하는 메서드
    """
    def __init__(self, items: list, reviews: list, token: str) -> None:
        self.items = items
        self.reviews = reviews
        self.token = token
        
    async def item_parse(self):
        result = []
        for _, item in enumerate(self.items["data"]):
            item_no = item["no"]
            item_name = item["name"]
            item_name_length = len(item["name"].replace(" ", ""))
            try:
                item_brand_name = re.compile(r"\[(.+)\](.+)").match(item["name"]).group(1)
            except AttributeError:
                item_brand_name = ""

            sales_price = item["sales_price"]
            discounted_price = item["discounted_price"]
            discount_rate = item["discount_rate"]
            short_description = item["short_description"]

            if item["sticker"]:
                for i in item["sticker"]["content"]:
                    if "%" in i["text"]:
                        is_applied_coupon = 1
                        coupon_discount_rate = re.sub(r"[^0-9]", "", i["text"])
                        break
            else:
                is_applied_coupon = 0
                coupon_discount_rate = 0
            
            if item["tags"]:
                tags = ";".join(j["name"] for j in item["tags"])
            else:
                tags = ""

            if item["delivery_type_names"]:
                delivery_type_names = ";".join(j for j in item["delivery_type_names"])
            else:
                delivery_type_names = ""

            category = ""
            purchase_benefits = ""

            client = KurlyClient(self.token)
            reviews_count = await client.get_review_count(item["no"])
            # reviews_count = ""
            result.append(
                [
                    str(item_no),
                    item_name,
                    item_name_length,
                    item_brand_name,
                    sales_price,
                    discounted_price,
                    discount_rate,
                    short_description,
                    is_applied_coupon,
                    coupon_discount_rate,
                    tags,
                    delivery_type_names,
                    category,
                    purchase_benefits,
                    reviews_count,
                ]
            )
        return result

    def review_parse(self):
        result = []
        for product in self.reviews:
            for item in product["data"]:
                
                review_elapsed_days = ((datetime.utcnow() + timedelta(hours=9)) - datetime.strptime(item["registeredAt"], "%Y-%m-%dT%H:%M:%S")).days

                if review_elapsed_days < 14:
                    continue
                
                product_no = item["contentsProductNo"]
                owner_grade = item["ownerGrade"]
                type = item["type"]
                contents = item["contents"]
                contents_length = len(item["contents"].replace(" ", ""))

                if item["images"]:
                    is_included_image = 1
                    image_count = len(item["images"])
                else:
                    is_included_image = 0
                    image_count = 0
                
                like_count = item["likeCount"]
                registered_at = datetime.strptime(item["registeredAt"], "%Y-%m-%dT%H:%M:%S")
                
                result.append(
                [
                    str(product_no),
                    owner_grade,
                    type,
                    contents,
                    contents_length,
                    is_included_image,
                    image_count,
                    like_count,
                    registered_at,
                    review_elapsed_days,
                ]
            )
    
        return result

    async def to_excel(self,file_name: str = "output"):
        parsed_items = await self.item_parse()
        item_data = pd.DataFrame(
            data=parsed_items,
            columns=[
                "상품코드",
                "상품명",
                "상품명 글자수",
                "브랜드명",
                "판매가",
                "할인가",
                "할인율",
                "소구문구",
                "쿠폰여부",
                "쿠폰할인율",
                "상품특징",
                "배송형태",
                "카테고리",
                "구매해택",
                "총 후기 개수",
            ],
        )

        parsed_reviews = self.review_parse()
        review_data = pd.DataFrame(
            data=parsed_reviews,
            columns=[
                "상품코드",
                "구매자 등급",
                "후기 베스트 여부",
                "리뷰 내용",
                "리뷰 글자 수",
                "사진 포함 여부",
                "사진 개수",
                "도움돼요 개수",
                "리뷰등록날짜",
                "리뷰등록경과일",
            ],
        )

        with pd.ExcelWriter(f"{file_name}.xlsx") as writer:
            item_data.index += 1
            review_data.index += 1
            item_data.to_excel(writer, sheet_name="Items")
            review_data.to_excel(writer, sheet_name="Reviews")

async def main():

    #Selenium Chrome driver setting
    driver = ChromeDriver(url="https://www.kurly.com/collections/market-best")
    #Kurly API에서 사용하는 access_token을 사용하기 위한 용도로 활용
    access_token = driver.get_bearer_token()


    #REST API
    request_client = KurlyClient(access_token)
    #Kurly의 베스트 상품 조회
    #per_page는 상품 개수를 뜻함
    best_items = await request_client.get_best_items(per_page=1)
    tasks = [request_client.get_reviews(product_no=item["no"]) for item in best_items["data"]]
    reviews = await asyncio.gather(*tasks)


    #Data 가공작업
    data = DataParse(best_items, reviews, access_token)
    await data.to_excel(file_name="item1")


if __name__ == "__main__":
    asyncio.run(main())
