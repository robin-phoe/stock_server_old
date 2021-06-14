# version 21.04.05
from django.http import HttpResponse
from django.shortcuts import render
from django.db import connection
from django.views.decorators.csrf import csrf_exempt
import datetime
import json
import re
import pandas as pd


def get_df_from_db(sql):
    cursor = connection.cursor()
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    # df = df.set_index(keys=['trade_date'])
    if 'trade_date' in df.columns:
        df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
    # df.reset_index(inplace=True)
    cursor.close()
    # print('df:',df)
    # df['trade_date'] = date2num(df['trade_date'])
    # print('df:', df[['avg_10', 'close_price']])
    return df


def sel_stock_list(sql):
    cursor = connection.cursor()
    cursor.execute(sql)
    res = cursor.fetchall()
    cursor.close()
    return res


def sel_stock_k_date(res, table, date_e=None, date_s='2020-08-01'):
    data_list = []
    stock_info = {}
    id_list = []
    for stock in res:
        # print('stock:',stock)
        id = stock[1]
        id_list.append(id)
        info = str(stock[0]) + ' ' + str(stock[2]) + ' ' + str(stock[3]) + ' ' + str(stock[5])
        tag = stock[0]
        stock_info[id] = (info, tag)
    rows_list = []
    id_tup = tuple(id_list)
    if date_e == None:
        sql = 'select stock_id,date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,point_type,0,0,0  ' \
              ' from stock_trade_data where stock_id in {0} and trade_date > "{1}" '.format(id_tup, date_s)
    else:
        sql = 'select stock_id,date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,point_type,0,0,0  ' \
              ' from stock_trade_data where stock_id in {0} and trade_date > "{1}" and trade_date <= "{2}" '.format(
            id_tup, date_s, date_e)
    print('sql:', sql)
    # 换成df
    # cursor = connection.cursor()
    # cursor.execute(sql)
    # rows = cursor.fetchall()
    # cursor.close()
    # # print('rows:',rows)
    # rows_list.extend(list(rows))
    # print('rows_list:',rows_list)

    trade_df = get_df_from_db(sql)
    # print("trade_df['wave_data']:", trade_df['wave_data'])
    # trade_df['wave_data'] = trade_df['wave_data'].apply(lambda x: 1 if float(x)>0 else 0)
    trade_df['point_type'] = trade_df['point_type'].apply(lambda x: 'n' if x == '' else x)
    rows_list = trade_df.values.tolist()

    stcok_dict = {}
    for tup in rows_list:
        if tup[0] not in stcok_dict:
            stcok_dict[tup[0]] = [list(tup[1:])]
        else:
            stcok_dict[tup[0]].append(list(tup[1:]))
    print('stcok_dict_len:', len(stcok_dict))
    for stock in stcok_dict:
        stcok_data = stcok_dict[stock]
        info = stock_info[stock][0]
        tag = stock_info[stock][1]
        rows_list = [table, tag, info, stcok_data]
        # data_list = {table,code(t_code | id),info(id,name,grade),[data]}
        data_list.append(rows_list)
    # print('data_list:',data_list)
    return data_list
    # print('row:',rows[i])
    # print(rows)


def del_stock(key, value):
    reason_type = re.findall('\D+', key)[1]
    print('reason_type:', key, reason_type)
    code = re.findall('\d+', key)[0]
    if reason_type == 'zhuang':
        sql = "UPDATE com_zhuang SET monitor = 0,reason = '{0}' where stock_id = '{1}'".format(value, code)
    elif reason_type == 'xiaoboxin':
        sql = "UPDATE remen_xiaoboxin SET monitor = 0,reason = '{0}' where trade_code = '{1}'".format(value, code)
    elif reason_type == 'remen_five':
        sql = "UPDATE com_redu_test SET monitor = 0,reason = '{0}' where trade_code = '{1}'".format(value, code)
    elif reason_type == 'monitor':
        sql = "UPDATE monitor SET monitor = 0,reason = '{0}' where trade_code = '{1}'".format(value, code)
    print('sql:', sql)
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()


def hello(request):
    return HttpResponse('hello world!')


# def deal_wave():
#     sql = "SELECT * FROM boxin_data"
#     wave_init_df = get_df_from_db(sql)
#
#     data = {'stock_id': [],
#             'trade_date': [],
#             'wave_price': [],
#             }
#     wave_df = pd.DataFrame(data)
#
#     #可使用二维列表转df优化，目前存储为三维[((),()),]
#     def wave_list_2_dict(raw):
#         data_list = raw['data_list']
#         wave_dict = {}
#         wave_list = json.loads(data_list)
#         for group_tuple in wave_list:
#             for day in group_tuple:
#                 wave_df.loc[len(wave_df)] = [raw[stock_id],day[0],day[1]]
#         return raw
#     wave_init_df.apply(wave_list_2_dict,axis=1)


@csrf_exempt
def runoob(request):
    context = {}
    context['data'] = []
    if request.method == "POST":
        # put_reson = json.loads(request.body)
        print('req:', request.POST)
        remen_xiaoboxin_param_dict = {}
        zhuang_param_dict = {}
        remen_5_param_dict = {}
        for key in request.POST:
            print('value:', key, request.POST[key])
            # del_stock(stock_info=key, reson=request.POST[key], db_field='stock_id', db_table='com_zhuang')
            # display处理
            if key[0:6] == 'reason':
                del_stock(key, request.POST[key])
            # respone处理
            elif key == 'monitor_input':
                sql = 'select  Z.trade_code,Z.stock_id,Z.stock_name,Z.grade,I.h_table,I.bk_name,Z.trade_code from monitor Z ' \
                      'inner join stock_informations I ' \
                      'on Z.stock_id = I.stock_id ' \
                      'where monitor = 1 and trade_date ="{0}"  '.format(request.POST[key])
                res = sel_stock_list(sql)
                # print('res:',res)
                data_list = sel_stock_k_date(res, table='monitor')
                print('data_list_len:', len(data_list))
                context[
                    'data'] = data_list  # [['2015-10-16',18.4,18.58,18.33,18.79,67.00,1,0.04,0.11,0.09],['2015-10-19',18.56,18.25,18.19,18.56,55.00,0,-0.00,0.08,0.09]]
                # context['hello'] = 'hello world!'
                # return render(request,'echarts_value_g.html',context)
                # return render(request, 'test_page.html', context)
            elif key in (
            'remen_xiaoboxin_B_input_date_s', 'remen_xiaoboxin_B_input_date_e', 'remen_xiaoboxin_B_today_input',
            'remen_xiaoboxin_B_input_grade_s', 'remen_xiaoboxin_B_input_grade_e'):
                remen_xiaoboxin_param_dict[key] = request.POST[key]

            elif key in ('zhuang_input_date_s', 'zhuang_input_date_e',
                         'zhuang_input_grade_s', 'zhuang_input_grade_e'):
                zhuang_param_dict[key] = request.POST[key]

            elif key in ('remen_5_date_s', 'remen_5_date_e', 'remen_5_today_input',
                         'remen_5_grade_s', 'remen_5_grade_e'):
                remen_5_param_dict[key] = request.POST[key]
            elif key == 'user_define':
                print('value:', request.POST[key])
                sql = request.POST[key]
                res = sel_stock_list(sql)
                # print('res:',res)
                data_list = sel_stock_k_date(res, table='user_define')
                print('data_list_len:', len(data_list))
                context['data'] = data_list
        if len(remen_xiaoboxin_param_dict) != 0:
            sql = 'select distinct Z.trade_code,Z.stock_id,Z.stock_name,Z.grade,I.h_table,I.bk_name,Z.trade_code from remen_xiaoboxin Z ' \
                  'left join stock_informations I ' \
                  'on Z.stock_id = I.stock_id ' \
                  'where monitor = 1 and grade >= "{0}" and grade <"{1}" and trade_date ="{2}" order by grade DESC'.format(
                remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_grade_s'],
                remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_grade_e'],
                remen_xiaoboxin_param_dict['remen_xiaoboxin_B_today_input'])
            res = sel_stock_list(sql)
            data_list = sel_stock_k_date(res, table='xiaoboxin',
                                         date_s=remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_date_s'],
                                         date_e=remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_date_e'])
            context['data'] = data_list
        elif len(zhuang_param_dict) != 0:
            sql = 'select distinct Z.stock_id,Z.stock_id,Z.stock_name,Z.zhuang_grade,I.h_table,I.bk_name from com_zhuang Z ' \
                  'left join stock_informations I ' \
                  'on Z.stock_id = I.stock_id ' \
                  'where monitor = 1 and zhuang_grade >= "{0}" and zhuang_grade <"{1}" order by zhuang_grade DESC'.format(
                zhuang_param_dict['zhuang_input_grade_s'],
                zhuang_param_dict['zhuang_input_grade_e'])
            res = sel_stock_list(sql)
            data_list = sel_stock_k_date(res, table='zhuang', date_s=zhuang_param_dict['zhuang_input_date_s'],
                                         date_e=zhuang_param_dict['zhuang_input_date_e'])
            context['data'] = data_list
        elif len(remen_5_param_dict) != 0:
            sql = 'select distinct Z.trade_code,Z.stock_id,Z.stock_name,Z.redu_5,I.h_table,I.bk_name from com_redu_test Z ' \
                  'left join stock_informations I ' \
                  'on Z.stock_id = I.stock_id ' \
                  'where monitor = 1 and redu_5 >= "{0}" and redu_5 <"{1}" and trade_date ="{2}" order by redu_5 DESC'.format(
                remen_5_param_dict['remen_5_grade_s'],
                remen_5_param_dict['remen_5_grade_e'], remen_5_param_dict['remen_5_today_input'])
            print('sql:', sql)
            res = sel_stock_list(sql)
            # print('date:',remen_5_param_dict['remen_5_date_s'],remen_5_param_dict['remen_5_date_e'])
            data_list = sel_stock_k_date(res, table='remen_five', date_s=remen_5_param_dict['remen_5_date_s'],
                                         date_e=remen_5_param_dict['remen_5_date_e'])
            context['data'] = data_list
        # reason = request.POST['reason']
        # re_res = re.findall('.*?reson(.*?)=(.*?)',bytes(request.body,encoding = "utf-8").decode())
        # print('re_res:',re_res)
    return render(request, 'echarts_value_g.html', context)
    # return render(request, 'html_base.html', context)
# def receive(request):
#     if request.POST:
#         print(request.POST.body)
#         reason = request.POST['reason']
#         # cursor.execute(up)
