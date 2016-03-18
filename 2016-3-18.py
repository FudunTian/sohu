#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-03-17 13:17:39
# Project: sohu
from pyspider.libs.base_handler import *

import datetime
from sqlalchemy import Column,String,create_engine,Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Sohu(Base):
    __tablename__ = 'sohu_{0}'.format(str(datetime.datetime.now()))
    id  = Column(Integer,primary_key=True)
    name = Column(String)
    rank =  Column(String)
    star = Column(String)
    number = Column(Integer)
    total = Column(String)
    area = Column(String)
    time = Column(String)


class Handler(BaseHandler):
    crawl_config = {
    }

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('tv.sohu.com/hotdrama/', callback=self.detail_page)
        self.crawl('tv.sohu.com/hotdrama/', callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http://tv.sohu.com/rank/"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    def init(self):
        ENGINE = create_engine('sqlite:///S:/ple/workspace/tianfd/practice/sohu.db')
        Base.metadata.create_all(ENGINE)
        session = sessionmaker()
        session.configure(bind = ENGINE)
        return  session()

    def trans2database(self,data):
        for sub_data in data:
            s = init()
            record = Sohu(
            name = sub_data['name'],
            rank =  sub_data['rank'],
            star = sub_data['star'],
            number = sub_data['number'],
            total = sub_data['total'],
            area = sub_data['area'],
            time = sub_data['time'],)
            s.add(record)
            s.commit()
        
    @config(priority=2)
    def detail_page(self, response):
        area = response.doc('ul > ul .on').text()
        time = response.doc('.rList_subMenu li').items()
        Name = response.doc('.vName').items()
        Rank = response.doc('.vRank').items()
        Star = response.doc('.vStar').items()
        Total=response.doc('.vTotal').items()
        Number=response.doc('.number').items()
        name_list = [sub.text() for sub in Name]
        rank_list = [sub.text() for sub in Rank]
        star_list = [sub.text() for sub in Star]
        number_list = [sub.text() for sub in Number]
        total_list = [sub.text() for sub in Total]
        final_list = []
        for j in time:
            for i in range(len(name_list)):
                cat = {
                    'name':name_list[i],
                   'rank':rank_list[i],
                   'star': star_list[i],
                   'number':number_list[i],
                   'total':total_list[i],
                   'time':j.text(),
                   'area':area
                   }
                final_list.append(cat)
        self.trans2database(final_list)
   
