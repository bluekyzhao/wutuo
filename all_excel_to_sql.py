# 一次性读取很多文件，全部写入数据库
# 现在一次性读取很多excel文件，其他功能以后用到再加
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine


class HandleData:
    def __init__(self, dirpath):
        """
        初始化的时候会直接 读取 dirpath 下面的所有文件
        合并到 self.res_df 这个dataframe
        """
        self.conn = create_engine(
            'mysql+pymysql://root:huajian@127.0.0.1:3306/tianyancha?charset=utf8')
        # 获取所有详细的文件路径，放到一个列表里面
        filenames = os.listdir(dirpath)
        self.filepaths = list(map(lambda x: dirpath + x, filenames))
        self.res_df = self._read_all_to_df()

    def _read_excel_to_df(self, file):
        # 单项具体功能
        return pd.read_excel(file)

    def _read_all_to_df(self):
        # 调度单项具体功能，主要做判断和处理
        """read all file to one dataframe"""
        # 多线程方式，对于IO密集型操作，效果极好
        with ThreadPoolExecutor(max_workers=50) as ex:
            df_generator = ex.map(self._read_excel_to_df, self.filepaths)
        # 所有的df，组成一个列表
        df_list = list(df_generator)
        res_df = pd.concat(df_list, axis=0, ignore_index=True)
        # res_df.describe()
        # 按照公司的名字去重
        res_df.drop_duplicates(subset='公司名称', keep='first', inplace=True)
        return res_df

    def files_to_mysql(self, table_name):
        self.res_df.to_sql(name=table_name, con=self.conn,
                           if_exists='append', method='multi', chunksize=1000)

    def sql2csv(self, table_name, csv_filename):
        # 从关系数据库里面读取表格，生成csv文件
        data = pd.read_sql_table(table_name, self.conn, chunksize=1000)
        data.to_csv(csv_filename)


hua = HandleData('C:/Users/blueky/Downloads/西安市-雁塔区-在业-有手机号码-无2019/')
hua.files_to_mysql('table_name')



# # 初始化连接引擎
# conn = create_engine(
#     'mysql+pymysql://root:huajian@127.0.0.1:3306/tianyancha?charset=utf8')


# # 获取所有详细的文件路径，放到一个列表里面
# dirname = 'C:/Users/blueky/Downloads/西安市-雁塔区-在业-有手机号码-无2019/'
# filenames = os.listdir(dirname)
# filepaths = list(map(lambda x: dirname+x, filenames))


# def read_excel_to_df(file):
#     return pd.read_excel(file)


# # 多线程方式 高速执行io操作
# with ThreadPoolExecutor(max_workers=50) as ex:
#     df_generator = ex.map(read_excel_to_df, filepaths)
# # 所有的df，组成一个列表
# df_list = list(df_generator)
# res_df = pd.concat(df_list, axis=0, ignore_index=True)
# # res_df.describe()
# # 按照公司的名字去重
# res_df.drop_duplicates(subset='公司名称', keep='first', inplace=True)
# # 写入数据库
# res_df.to_sql(name='yanta', con=conn,
#               if_exists='append', method='multi', chunksize=1000)
