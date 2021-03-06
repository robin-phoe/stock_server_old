# version 21.04.05
from django.http import HttpResponse
from django.shortcuts import render
from django.db import connection
from django.views.decorators.csrf import csrf_exempt
import datetime
import json
import re
import pandas as pd
import pub_uti


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
    if len(id_list) == 0:
        return []
    elif len(id_list) == 1:
        #单个元素转换为tuple sql查询时会报错
        id_list.append(id_list[0])
    id_tup = tuple(id_list)
    #板块展示特例
    print('table:',table,id_tup)
    if date_e == None:
        sql = 'select stock_id,date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,point_type,0,0,0  ' \
              ' from stock_trade_data where stock_id in {0} and trade_date > "{1}" '.format(id_tup, date_s)
        if table == 'bankuai_day_data':
            sql = 'select bk_code as stock_id,date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,"" as point_type,0,0,0  ' \
                  ' from bankuai_day_data where bk_code in {0} and trade_date > "{1}" '.format(id_tup, date_s)
    else:
        sql = 'select stock_id,date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,point_type,0,0,0  ' \
              ' from stock_trade_data where stock_id in {0} and trade_date > "{1}" and trade_date <= "{2}" '.format(
            id_tup, date_s, date_e)
        if table == 'bankuai_day_data':
            sql = 'select bk_code as stock_id,date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,"" as point_type,0,0,0  ' \
                  ' from bankuai_day_data where bk_code in {0} and trade_date > "{1}" and trade_date <= "{2}"'.format(id_tup, date_s, date_e)
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
    sql_area = "select stock_id,zhuang_section from com_zhuang where stock_id in {}".format(id_tup)
    zhuang_df = get_df_from_db(sql_area)
    area_df = pd.DataFrame(columns=('stock_id', 'start_date', 'end_date', 's_price','e_price'))
    def split_section(raw):
        section_list = eval(raw['zhuang_section'])
        if len(section_list) == 0:
            return raw
        for sec_tup in section_list:#('2018-05-28 00:00:00', '2018-03-16 00:00:00')
            area_df.loc[len(area_df)] = [raw['stock_id'],sec_tup[1][0:10],sec_tup[0][0:10],0,0]
    zhuang_df.apply(split_section,axis =1)
    price_df = trade_df[['stock_id','trade_date','close_price']]
    area_df = pd.merge(area_df,price_df,how='left',left_on=['stock_id','start_date'],right_on = ['stock_id','trade_date'])
    area_df['s_price'] = area_df['close_price']
    del area_df['close_price']
    area_df = pd.merge(area_df,price_df,how='left',left_on=['stock_id','end_date'],right_on = ['stock_id','trade_date'])
    area_df['e_price'] = area_df['close_price']
    area_df.fillna(0,inplace = True)
    del area_df['close_price']
    # [[{"xAxis": 284, "yAxis": "19.65"}, {"xAxis": 290, "yAxis": "17.70"}],
    #  [{"xAxis": 290, "yAxis": "17.70"}, {"xAxis": 299, "yAxis": "17.98"}]]
    zhuang_area_dict = {}
    def write_area(raw):
        if raw['stock_id'] not in zhuang_area_dict:
            zhuang_area_dict[raw['stock_id']] = []
        zhuang_area_dict[raw['stock_id']].append([{"xAxis": raw['start_date'], "yAxis": raw['s_price']*0.9}, {"xAxis": raw['end_date'], "yAxis": raw['e_price']*1.1}])
    area_df.apply(write_area,axis=1)

    rows_list = trade_df.values.tolist()
    stcok_dict = {}
    for tup in rows_list:
        if tup[0] not in stcok_dict:
            stcok_dict[tup[0]] = [list(tup[1:])]
        else:
            stcok_dict[tup[0]].append(list(tup[1:]))
    print('stcok_dict_len:', len(stcok_dict))
    for stock in stcok_dict:
        stock_data = stcok_dict[stock]
        info = stock_info[stock][0]
        tag = stock_info[stock][1]
        if stock in zhuang_area_dict:
            zhuang_area = zhuang_area_dict[stock]
        else:
            zhuang_area =[]
        rows_list = [table, tag, info, stock_data, zhuang_area]
        # data_list = {table,code(t_code | id),info(id,name,grade),[data]}
        data_list.append(rows_list)
    print('data_list:',data_list[0])
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
    elif reason_type == 'limit_up_single':
        sql = "UPDATE limit_up_single SET monitor = 0,reason = '{0}' where trade_code = '{1}'".format(value, code)
    elif reason_type == 'remen_retracement':
        sql = "UPDATE remen_retracement SET monitor = 0,reason = '{0}' where trade_code = '{1}'".format(value, code)
    elif reason_type == 'remen_boxin':
        sql = "UPDATE remen_boxin SET monitor = 0,reason = '{0}' where trade_code = '{1}'".format(value, code)
    elif reason_type == 'bk':
        sql = "UPDATE bankuai_day_data SET reason = '{0}' where trade_code = '{1}'".format(value, code)
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
    context['bk'] = []

    if request.method == "POST":
        # put_reson = json.loads(request.body)
        print('req:', request.POST)
        remen_xiaoboxin_param_dict = {}
        zhuang_param_dict = {}
        zhuang_day_param_dict = {}
        remen_5_param_dict = {}
        limit_up_param_dict = {}
        remen_retrace_param_dict = {}
        remen_boxin_param_dict = {}
        bk_param_dict = {}
        bk_summary = {}
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
                context['data'] = data_list  # [['2015-10-16',18.4,18.58,18.33,18.79,67.00,1,0.04,0.11,0.09],['2015-10-19',18.56,18.25,18.19,18.56,55.00,0,-0.00,0.08,0.09]]
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
            elif key in ('zhuang_day_input_date_s', 'zhuang_day_input_date_e',
                         'zhuang_day_input_grade_s', 'zhuang_input_day_grade_e','zhuang_day_today_input'):
                zhuang_day_param_dict[key] = request.POST[key]

            elif key in ('remen_5_date_s', 'remen_5_date_e', 'remen_5_today_input',
                         'remen_5_grade_s', 'remen_5_grade_e'):
                remen_5_param_dict[key] = request.POST[key]
            elif key in ('limit_up_date_s', 'limit_up_date_e', 'limit_up_today_input',
                         'limit_up_grade_s', 'limit_up_grade_e'):
                limit_up_param_dict[key] = request.POST[key]
            elif key in ('remen_retrace_date_s', 'remen_retrace_date_e', 'remen_retrace_today_input',
                         'remen_retrace_grade_s', 'remen_retrace_grade_e'):
                remen_retrace_param_dict[key] = request.POST[key]
            elif key in ('remen_boxin_date_s', 'remen_boxin_date_e', 'remen_boxin_today_input',
                             'remen_boxin_grade_s', 'remen_boxin_grade_e'):
                remen_boxin_param_dict[key] = request.POST[key]
            elif key in ('bk_date_s', 'bk_date_e', 'bk_today_input',
                         'bk_grade_s', 'bk_grade_e','bk_name'):
                bk_param_dict[key] = request.POST[key]
            elif key in ('bk_data_date_s','bk_data_date_e','bk_data_grade'):
                bk_summary[key] = request.POST[key]
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
        elif len(zhuang_day_param_dict) != 0:
            sql = 'select distinct Z.trade_code,Z.stock_id,I.stock_name,Z.grade,I.h_table,I.bk_name from zhuang_day_grade Z ' \
                  'left join stock_informations I ' \
                  'on Z.stock_id = I.stock_id ' \
                  'where com_date = "{}" order by grade DESC'.format(zhuang_day_param_dict['zhuang_day_today_input'])
            res = sel_stock_list(sql)
            data_list = sel_stock_k_date(res, table='zhuang_day', date_s=zhuang_day_param_dict['zhuang_day_input_date_s'],
                                         date_e=zhuang_day_param_dict['zhuang_day_input_date_e'])
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
        if len(limit_up_param_dict) != 0:
            sql = 'select distinct Z.trade_code,Z.stock_id,Z.stock_name,Z.grade,I.h_table,I.bk_name,Z.trade_code from limit_up_single Z ' \
                  'left join stock_informations I ' \
                  'on Z.stock_id = I.stock_id ' \
                  'where monitor = 1 and grade >= "{0}" and grade <"{1}" and trade_date ="{2}" order by grade DESC'.format(
                limit_up_param_dict['limit_up_grade_s'],
                limit_up_param_dict['limit_up_grade_e'],
                limit_up_param_dict['limit_up_today_input'])
            res = sel_stock_list(sql)
            data_list = sel_stock_k_date(res, table='limit_up_single',
                                         date_s=limit_up_param_dict['limit_up_date_s'],
                                         date_e=limit_up_param_dict['limit_up_date_e'])
            context['data'] = data_list
        if len(remen_retrace_param_dict) != 0:
            sql = 'select distinct Z.trade_code,Z.stock_id,Z.stock_name,Z.grade,I.h_table,I.bk_name,Z.trade_code from remen_retracement Z ' \
                  'left join stock_informations I ' \
                  'on Z.stock_id = I.stock_id ' \
                  'where monitor = 1 and grade >= "{0}" and grade <"{1}" and trade_date ="{2}" order by grade DESC'.format(
                remen_retrace_param_dict['remen_retrace_grade_s'],
                remen_retrace_param_dict['remen_retrace_grade_e'],
                remen_retrace_param_dict['remen_retrace_today_input'])
            res = sel_stock_list(sql)
            data_list = sel_stock_k_date(res, table='remen_retracement',
                                         date_s=remen_retrace_param_dict['remen_retrace_date_s'],
                                         date_e=remen_retrace_param_dict['remen_retrace_date_e'])
            context['data'] = data_list
        if len(remen_boxin_param_dict) != 0:
            sql = 'select distinct Z.trade_code,Z.stock_id,Z.stock_name,Z.grade,I.h_table,I.bk_name,Z.trade_code from remen_boxin Z ' \
                  'left join stock_informations I ' \
                  'on Z.stock_id = I.stock_id ' \
                  'where monitor = 1 and grade >= "{0}" and grade <"{1}" and trade_date ="{2}" order by grade DESC'.format(
                remen_boxin_param_dict['remen_boxin_grade_s'],
                remen_boxin_param_dict['remen_boxin_grade_e'],
                remen_boxin_param_dict['remen_boxin_today_input'])
            res = sel_stock_list(sql)
            data_list = sel_stock_k_date(res, table='remen_boxin',
                                         date_s=remen_boxin_param_dict['remen_boxin_date_s'],
                                         date_e=remen_boxin_param_dict['remen_boxin_date_e'])
            context['data'] = data_list
        if len(bk_param_dict) != 0:
            # sql = 'select distinct Z.bk_id,Z.bk_code,Z.bk_name,Z.redu,Z.amount,Z.amount,Z.bk_id from bankuai_day_data Z ' \
            #       'where grade >= "{0}" and grade <"{1}" and trade_date ="{2}" order by grade DESC'.format(
            #     bk_param_dict['bk_grade_s'],
            #     bk_param_dict['bk_grade_e'],
            #     bk_param_dict['bk_today_input'])
            bk_name = bk_param_dict['bk_name']
            sql = 'select distinct Z.bk_id,Z.bk_code,Z.bk_name,Z.redu,Z.amount,Z.amount,Z.bk_id from bankuai_day_data Z ' \
                  'where  trade_date ="{0}" order by redu DESC'.format(
                bk_param_dict['bk_today_input'])
            if bk_name != '':
                sql = 'select distinct Z.bk_id,Z.bk_code,Z.bk_name,Z.redu,Z.amount,Z.amount,Z.bk_id from bankuai_day_data Z ' \
                      'where  trade_date ="{0}" and bk_name like "%{1}%"'.format(
                    bk_param_dict['bk_today_input'],bk_name)
            res = sel_stock_list(sql)
            data_list = sel_stock_k_date(res, table='bankuai_day_data',
                                         date_s=bk_param_dict['bk_date_s'],
                                         date_e=bk_param_dict['bk_date_e'])
            context['data'] = data_list
        if len(bk_summary) != 0:
            sql = "select trade_date,bk_name,bk_code,ranks,increase from bankuai_day_data " \
                  "where trade_date >= '{0}' and trade_date <='{1}' ".format(bk_summary['bk_data_date_s'],bk_summary['bk_data_date_e'])
            df = pub_uti.creat_df(sql,ascending=True)
            # print('df:',df)
            df['ranks'] = df['increase']
            bk_dict = {}
            bk_set = set(df['bk_name'])
            time_list = []
            for bk_name in bk_set:
                single_df = df[df.bk_name == bk_name]
                single_df = single_df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
                ranks_list = single_df['ranks'].to_list()
                if min(ranks_list) > int(bk_summary['bk_data_grade']):
                    continue
                bk_dict[bk_name] = ranks_list
                time_list = single_df['trade_date'].to_list()
            print(time_list)
            bk_summary = {'time':time_list,'data':bk_dict}
            context['bk'] = bk_summary

    return render(request, 'echarts_value_g.html', context)
    # return render(request, 'html_base.html', context)
# def receive(request):
#     if request.POST:
#         print(request.POST.body)
#         reason = request.POST['reason']
#         # cursor.execute(up)
