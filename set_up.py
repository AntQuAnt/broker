# set up database, KIS api
from dotenv import load_dotenv
import os
import pymysql

# get env
load_dotenv(os.path.join(os.getcwd(), '.env'))

# Database
con = pymysql.connect(
    user = os.environ['DB_user'],
    passwd = os.environ['DB_passwd'],
    host = os.environ['DB_host'],
    charset = 'utf8',
    client_flag = pymysql.constants.CLIENT.MULTI_STATEMENTS
)
cursor = con.cursor()

query = '''
    drop database if exists broker;
    create database broker;
    use broker; 

    create table stock_code (
        code char(6),
        name varchar(45) not null,
        market varchar(45) not null,
        
        primary key(code)
    );

    create table kospi_price (
        code char(6) not null,
        date datetime not null,
        open int,
        close int,
        high int,
        low int,
        volume int,
        
        primary key(code, date),
        foreign key(code) references stock_code(code)
    );

    create table kosdaq_price (
        code char(6) not null,
        date datetime not null,
        open int,
        close int,
        high int,
        low int,
        volume int,
        
        primary key(code, date),
        foreign key(code) references stock_code(code)
    ); 

    create table balance_sheet (
        code char(6) not null,
        date datetime not null, 
        유동자산 bigint,
        비유동자산 bigint,
        자산총계 bigint,
        유동부채 bigint,
        비유동부채 bigint,
        부채총계 bigint,
        이익잉여금 bigint,
        자본총계 bigint,
        매출액 bigint,
        영업이익 bigint,
        법인세차감전_순이익 bigint,
        당기순이익 bigint,
        당기순이익_손실 bigint,
        자본금 bigint,
        
        primary key(code, date),
        foreign key(code) references stock_code(code)
    );
'''

cursor.execute(query)
con.close()