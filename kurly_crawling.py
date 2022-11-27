"""
Selenium을 활용하여 데이터 수집
"""


import re
import time
from typing import Tuple

from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys

import pandas as pd

def chrome_driver(display_mode: bool = False):
    ####### Selenium Drive setting #######
    # 4.0 버전 이상에서는 Choromdriver 사용 시 Service object를 넘기는걸 권장
    # https://www.codeit.kr/community/threads/33729
    options = webdriver.ChromeOptions()  # 크롬 옵션 객체 생성
    if not display_mode:
   
        # 속도 향상을 위한 옵션 해제
        prefs = {
            "profile.default_content_setting_values": {
                "cookies": 2,
                "images": 2,
                "plugins": 2,
                "popups": 2,
                "geolocation": 2,
                "notifications": 2,
                "auto_select_certificate": 2,
                "fullscreen": 2,
                "mouselock": 2,
                "mixed_script": 2,
                "media_stream": 2,
                "media_stream_mic": 2,
                "media_stream_camera": 2,
                "protocol_handlers": 2,
                "ppapi_broker": 2,
                "automatic_downloads": 2,
                "midi_sysex": 2,
                "push_messaging": 2,
                "ssl_cert_decisions": 2,
                "metro_switch_to_desktop": 2,
                "protected_media_identifier": 2,
                "app_banner": 2,
                "site_engagement": 2,
                "durable_storage": 2,
            },
        }
        # options.add_experimental_option("prefs", prefs)

        options.add_argument("headless")  # headless 모드 설정
        options.add_argument("window-size=1920x1080")  # 화면크기(전체화면)
        options.add_argument("disable-gpu")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")

    # caps = DesiredCapabilities().CHROME
    # caps["pageLoadStrategy"] = "none"


    service = Service("chromedriver")
    return webdriver.Chrome(service=service, options=options)

def crawling(driver: webdriver.Chrome, item_num: int, page_num: int) -> Tuple[list, list, list]:
    """
    [마켓컬리 Class Names]

    베스트 상품 리스트 : css-1xyd46f
    개별 상품 : css-rklo75
    상품 요약 : css-13g46z0
    쿠폰 발행 여부 : css-y4sfl6
    할인 적용 여부 : discount-rate
    상품 키워드 : css-1hrnl0u
    상품 리뷰 리스트 : css-169773r
    사용자 등급 : XPATH
    베스트 리뷰 : XPATH
    리뷰 내용 : css-i69j0n
    리뷰 사진 : css-1l7ac3a
    도움돼요 : css-g3a39p
    리뷰 등록일자 : css-14kcwq8
    리뷰 다음 페이지 버튼 : css-jz9m4p


    [변수 정보]
    :driver : webdriver
    - chromedriver로 마켓컬리 웹페이지 역할을 함

    :item_num: int
    - 크롤링하고 싶은 상품 개수 입력 

    :page_num: int 
    - 마켓컬리에서는 한 페이지 당 리뷰를 10개 씩 보여줌
    - 페이지 번호를 통해 크롤링 할 데이터의 개수를 지정할 수 있음
        - (ex. page_num = 2 면 해당 상품은 20개의 데이터를 추출)
    """    

    # 마켓컬리 베스트 상품 페이지
    url = "https://www.kurly.com/collections/market-best"
    driver.get(url)
    driver.implicitly_wait(time_to_wait=3)

    error = []
    item_list = []
    review_list = []
    
    # 제품 정보
    items = driver.find_elements(By.CLASS_NAME, "css-1xyd46f")
    for item_idx in range(len(items)):
        
        if item_idx == item_num:
            break
        
        # 상품 상세보기 이동 후, 다시 돌아오면 element 사용에 오류 발생
        # 따라서, 반복문 시작 시 elemets를 새로 호출
        # 참조 : https://velog.io/@jo1132/220909-%EB%A7%88%EC%BC%93%EC%BB%AC%EB%A6%AC-%ED%81%AC%EB%A1%A4%EB%A7%81%ED%95%98%EA%B8%B0
        items = driver.find_elements(By.CLASS_NAME, "css-1xyd46f")
        item = items[item_idx]

        item_name = item.find_element(By.CLASS_NAME, "css-rklo75").text
        img_url = item.find_element(By.TAG_NAME, "img").get_attribute("src")
        description = item.find_element(By.CLASS_NAME, "css-13g46z0").text
        coupon_elements = item.find_elements(By.CLASS_NAME, "css-y4sfl6")
        if coupon_elements:
            issued_coupon = 1
            coupon_discount_rate = re.sub(r"[^0-9]", "", coupon_elements[0].text)
        else:
            issued_coupon = 0
            coupon_discount_rate = "0"

        discount_elements = item.find_elements(By.CLASS_NAME, "discount-rate")
        if discount_elements:
            is_discounted = 1
            discount_rate = re.sub(r"[^0-9]", "", discount_elements[0].text)
        else:
            is_discounted = 0
            discount_rate = "0"

        keywords = item.find_elements(By.CLASS_NAME, "css-1hrnl0u")
        if keywords:
            keywords = ";".join(keywords[0].text.split("\n"))
        else:
            keywords = ""

        item_list.append(
            [
                item_name,
                img_url,
                description,
                issued_coupon,
                coupon_discount_rate,
                is_discounted,
                discount_rate,
                keywords,
            ]
        )

        # 상품 클릭
        item.click()
        try:
            # 리뷰 페이지의 elements 호출
            for current_page_num in range(1, page_num + 1):     
                reviews = driver.find_elements(By.CLASS_NAME, "css-169773r")
                for review_idx in range(len(reviews)):
    
                    review_date = reviews[review_idx].find_element(By.CLASS_NAME, "css-14kcwq8").text
                    review_elapsed_days = ((datetime.utcnow() + timedelta(hours=9)) - datetime.strptime(review_date, "%Y.%m.%d")).days

                    if review_elapsed_days < 14:
                        continue

                    user_info = reviews[review_idx].find_element(By.XPATH, f'//*[@id="review"]/section/div[2]/div[{review_idx+5}]/div/div').text.split("\n")
                    if len(user_info) == 3:
                        is_best_review = 1
                        user_level = user_info[1]
                    else:
                        is_best_review = 0
                        user_level = user_info[0]
                    review_content = "".join(reviews[review_idx].find_element(By.CLASS_NAME, "css-i69j0n").text.split("\n"))
                    string_count = len(review_content.replace(" ", ""))

                    photos = reviews[review_idx].find_elements(By.CLASS_NAME, "css-1l7ac3a")
                    if photos:
                        is_included_photos = 1
                        photo_num = str(len(photos[0].find_elements(By.TAG_NAME, "button")))
                    else:
                        is_included_photos = 0
                        photo_num = "0"

                    is_included_photos = 1 if reviews[review_idx].find_elements(By.XPATH, f'//*[@id="review"]/section/div[2]/div[{review_idx+5}]/article/div/div[2]') else 0
                    help_num = re.sub(r"[^0-9]", "", reviews[review_idx].find_element(By.CLASS_NAME, "css-g3a39p").text)
                    if not help_num:
                        help_num = "0"
                    review_list.append(
                        [
                            item_idx + 1,
                            user_level,
                            is_best_review,
                            review_content,
                            string_count,
                            is_included_photos,
                            photo_num,
                            help_num,
                            review_date,
                            str(review_elapsed_days),
                        ]
                    )
                if current_page_num == page_num:
                    break
        
                driver.find_element(By.XPATH, '//*[@id="review"]/section/div[2]/div[15]/button[2]').send_keys(Keys.ENTER)
                time.sleep(1)

        except Exception as e:
            error.append({"item_id": item_idx+1, "item_name": item_name})


        # 브라우져 뒤로가기
        driver.execute_script("window.history.go(-1)")
    
    return item_list, review_list, error

def data_to_excel(item_list:list, review_list:list, file_name: str = "output"):
    item_data = pd.DataFrame(
        data=item_list,
        columns=[
            "제품명",
            "URL",
            "제품요약",
            "쿠폰여부",
            "쿠폰할인율",
            "제품할인여부",
            "제품할인율",
            "제품키워드",
        ],
    )

    review_data = pd.DataFrame(
        data=review_list,
        columns=[
            "제품아이디",
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

def main():
    driver = chrome_driver(display_mode=True)

    start = time.time()
    item_list, review_list, error = crawling(driver=driver, item_num=1, page_num=10)
    data_to_excel(item_list, review_list, file_name="test_20221119")

    # 작업 완료 후 드라이버 종료
    driver.close()
    
    if error:
        print(error)

    print(time.time() - start)


if __name__ == "__main__":
    main()