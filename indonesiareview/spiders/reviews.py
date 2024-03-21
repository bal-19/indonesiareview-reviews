from datetime import datetime
from pathlib import Path
from uuid import uuid4
import logging
import scrapy
import json
import s3fs
import re
import os


class ReviewsSpider(scrapy.Spider):
    name = 'reviews'
    total_success = 0
    total_failed = 0
    # =================================================================================
    category_reviews = 'lain-lain'
    # =================================================================================
    
    def start_requests(self):
        # =================================================================================
        url = "https://indonesiareview.co.id/keuangan/bca-bank-central-asia-pengalaman"
        # =================================================================================
        yield scrapy.Request(url=url, callback=self.parse)
        
    # =================================================================================
    def upload_to_s3(self, rpath, lpath):
        client_kwargs = {
            'key': 'YOUR_S3_KEY',
            'secret': 'YOUR_S3_SECRET_KEY',
            'endpoint_url': 'YOUR_S3_ENDPOINT',
            'anon': False
        }

        s3 = s3fs.core.S3FileSystem(**client_kwargs)

        # Upload file
        s3.upload(rpath=rpath, lpath=lpath)
    # =================================================================================
            
    def log_error(self, crawling_time, id_project, project, sub_project, source_name, sub_source_name, id_sub_source, id_data, process_name, status, type_error, message, assign, path):
        log_error = {
            "crawlling_time": crawling_time,
            "id_project": id_project,
            "project": project,
            "sub_project": sub_project,
            "source_name": source_name,
            "sub_source_name": sub_source_name,
            "id_sub_source": id_sub_source,
            "id_data": id_data,
            "process_name": process_name,
            "status": status,
            "type_error": type_error,
            "message": message,
            "assign": assign
        }
        
        try:
            with open(path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            existing_data = []

        existing_data.append(log_error)

        with open(path, 'w') as file:
            json.dump(existing_data, file)
            
            
    def log(self, crawling_time, id_project, project, sub_project, source_name, sub_source, id_sub_source, total, total_success, total_failed, status, assign, path):
        log = {
            'crawling_time': crawling_time,
            'id_project': id_project,
            'project': project,
            'sub_project': sub_project,
            'source_name': source_name,
            'sub_source_name': sub_source,
            'id_sub_source': id_sub_source,
            'total_data': int(total),
            'total_success': total_success,
            'total_failed': total_failed,
            'status': status,
            'assign': assign,
        }
        
        try:
            with open(path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            existing_data = []

        existing_data.append(log)

        with open(path, 'w') as file:
            json.dump(existing_data, file)
        
        
    def parse(self, response):
        category_reviews = self.category_reviews
        url = response.url
        domain = url.split('/')[2]
        sub_source = url.split('/')[3]
        id_sub_src = int(str(uuid4()).replace('-', ''), 16)
        crawling_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        crawling_time_epoch = int(datetime.now().timestamp() * 1000)
        # ======================================================================================================
        # YOUR S3 PATH
        path_data_raw = f's3://{domain}/{sub_source}/json'
        path_data_clean = f's3://{domain}/{sub_source}/json'
        # ======================================================================================================
        
        # logging
        id_project = None
        project = 'data intelligence'
        sub_project = 'data review'
        assign = 'iqbal'
        
        # scraping company information
        company = response.css('#side-content-desktop > header > h2:nth-child(1)::text').get()
        total_reviews = response.css('#laatste-reviews > header > h2::text').get()
        total_reviews = int(re.findall(r'\d+', total_reviews)[0])
        buy_again_percent = response.css('#header-company > div:nth-child(1) > section > div > div.banner-main.layout__main.bird-rating-block > div.info-item.banner-score > h4::text').get()
        buy_again_percent = buy_again_percent.split(' ')[0]
        total_rating = response.css('#header-company > div:nth-child(1) > section > div > div.banner-main.layout__main.bird-rating-block > div.info-item.banner-score > div::attr(class)').get()
        total_rating = float(total_rating.split(' ')[-1].replace('rating-',''))/10
        
        detail_total_rating = []
        
        # detail total rating
        for rating in response.css('#header-company > div:nth-child(1) > section > div > div.banner-main.layout__main.bird-rating-block > div.ratings-container > div.info-item'):
            score_rating = rating.css('div.info-text::text').get()
            category_rating = rating.css('div.info-text > strong::text').get()
            category_rating = float(category_rating)
            
            detail_total_rating.append({
                'score_rating' : score_rating,
                'category_rating' : category_rating
            }) 
        
        # scraping review
        for review in response.css('#js-reviews-list > li'):
            try:
                detail_reviews_rating = []
                reply_content_reviews = []
                id_review = review.css('li::attr(id)').get()
                if id_review == 'js-reviews-empty':
                    continue
                else:
                    nickname = review.css('div.review-container.layout__main.bird-review-container > section.review-reply > div > div > figure > figcaption > a::text').get()
                    created_time = review.css('div.review-container.layout__main.bird-review-container > section.review-reply > div > div > figure > figcaption > time::attr(datetime)').get()
                    created_time_datetime = datetime.strptime(created_time, "%Y-%m-%d %H:%M:%S")
                    created_time_epoch = int(created_time_datetime.timestamp() * 1000)
                    title_detail_review = review.css('div.review-container.layout__main.bird-review-container > section.review-reply > div > h3 > strong.review-reply-title::text').get()
                    reviews_rating = review.css('div.reviews__ratings.layout__aside > div > div.ratings-block__total > div::attr(class)').get()
                    reviews_rating = float(reviews_rating.split(' ')[-1].replace('rating-',''))/10
                    buy_again = review.css('div.reviews__ratings.layout__aside > div > div.ratings-block__container > div.ratings-block__bottom > div::attr(data-value)').get()
                    total_likes = review.css('div.review-container.layout__main.bird-review-container > section.review-reply > div.inner-wrapper > div.review-reply__footer > div.review-reply__assets > a.review-nuttig-click > span::text').get()
                    total_reply = review.css('div.review-container.layout__main.bird-review-container > section.review-reply > div.inner-wrapper > div.review-reply__footer > div.review-reply__assets > a.show-reviews > span::text').get()
                    content_review = review.css('div.review-container.layout__main.bird-review-container > section.review-reply > div > p::text').get()
                    
                    # detail reviews rating
                    for review_rating in review.css('div.reviews__ratings.layout__aside > div > div.ratings-block__container > div.ratings-block__block'):
                        category_rating_review = review_rating.css('p::text').get()
                        score_rating_review = review_rating.css('div.rating-stars::attr(data-value)').get()
                        score_rating_review = float(score_rating_review)
                        
                        detail_reviews_rating.append({
                            'score_rating' : score_rating_review,
                            'category_rating' : category_rating_review
                        })
                        
                    # reply reviews
                    reply_container = review.css(f'div.review-container.layout__main.bird-review-container > section.review-reply.review-reply__company.review-comment-{id_review}')
                    if reply_container is not None:
                        for reply in reply_container:
                            username_reply = reply.css('div > div > figure > div.user-tag__info > figcaption > a::text').get()
                            content_reply = reply.css('div > p::text').get()
                            
                            reply_content_reviews.append({
                                'username_reply_reviews' : username_reply,
                                'content_reviews' : content_reply
                            })

                file_name = f'{company.replace(" ", "_").lower()}_{id_review}_{created_time_epoch}.json'
                
                # saving to json variabel
                company_information = {
                    'link' : url,
                    'domain' : domain,
                    'tag' : [domain, category_reviews, company],
                    'crawling_time' : crawling_time,
                    'crawling_time_epoch' : crawling_time_epoch,
                    'path_data_raw' : f'{path_data_raw}/{file_name}',
                    'path_data_clean' : f'{path_data_clean}/{file_name}',
                    'reviews_name' : company,
                    'location_reviews' : None,
                    'category_reviews' : category_reviews,
                    'total_reviews' : total_reviews,
                    'buy_again' : buy_again_percent,
                    'reviews_rating' : {
                        'total_rating' : total_rating,
                        'detail_total_rating' : detail_total_rating
                    }
                }

                # saving to json variabel
                data_reviews = {
                    'detail_reviews' : {
                        'id_review' : id_review,
                        'username_reviews' : nickname,
                        'image_reviews': None,
                        'created_time' : created_time,
                        'created_time_epoch' : created_time_epoch,
                        'email_reviews' : None,
                        'company_name' : company,
                        'title_detail_review' : title_detail_review,
                        'reviews_rating' : reviews_rating,
                        'buy_again' : buy_again,
                        'detail_reviews_rating' : detail_reviews_rating,
                        'total_likes_reviews' : int(total_likes),
                        'total_dislikes_reviews' : None,
                        'total_reply_reviews' : int(total_reply),
                        'content_reviews' : content_review,
                        'reply_content_reviews' : reply_content_reviews,
                        'date_of_experience' : created_time,
                        'date_of_experience_epoch' : created_time_epoch
                    }
                }

                data = {**company_information, **data_reviews}
                
                # =================================================================================
                # YOUR LOCAL PATH
                my_dir = 'F:/Work/Crawling Indonesia Review/data'
                # =================================================================================
                if not os.path.exists(my_dir):
                    os.makedirs(my_dir)
                    
                with open(f'{my_dir}/{file_name}', 'w') as f:
                    json.dump(data, f)
                    
                # write file to s3
                self.upload_to_s3(f'{path_data_raw.replace('s3://', '')}/{file_name}', f'{my_dir}/{file_name}')
                # end of write file to s3
                
                self.total_success += 1
                # ========================================================================================================================
                self.log_error(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, id_review, 'crawling', 'success', '', '', assign, 'F:/Work/Crawling Indonesia Review/log_error.json')
                # ========================================================================================================================
                
            except Exception as e:
                self.total_failed += 1
                # ========================================================================================================================
                self.log_error(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, id_review, 'crawling', 'error', type(e).__name__, str(e), assign, 'F:/Work/Crawling Indonesia Review/log_error.json')
                # ========================================================================================================================
        
        # pagination
        next_page = response.css('#laatste-reviews > div > ul > li').getall()
        if not next_page:
            # ========================================================================================================================
            self.log(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, total_reviews, self.total_success, self.total_failed, 'done', assign, 'F:/Work/Crawling Indonesia Review/log.json')
            # ========================================================================================================================
        else:
            next_page = next_page[-1]
            next_page_link = scrapy.Selector(text=next_page).css('a::attr(href)').get()
            aria_disabled_value = scrapy.Selector(text=next_page).css('li::attr(aria-disabled)').get()
            if aria_disabled_value == 'false':
                yield scrapy.Request(url=response.urljoin(next_page_link), callback=self.parse)
            else:
            # ========================================================================================================================
                self.log(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, total_reviews, self.total_success, self.total_failed, 'done', assign, 'F:/Work/Crawling Indonesia Review/log.json')
            # ========================================================================================================================