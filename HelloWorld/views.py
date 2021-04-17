#version 21.04.05
from django.http import HttpResponse
from django.shortcuts import render
from django.db import connection
from django.views.decorators.csrf import csrf_exempt
import datetime
import json
import re
cursor = connection.cursor()
def sel_stock_list(cursor,sql):
    # sql = 'select distinct Z.stock_id,Z.stock_name,Z.zhuang_grade,I.h_table,I.bk_name from com_zhuang Z '\
    #                'left join stock_informations I '\
    #                'on Z.stock_id = I.stock_id '\
    #                'where zhuang_grade >= 1000 and zhuang_grade <10000 '
    # sql = 'select distinct Z.stock_id,Z.stock_name,Z.redu_5,I.h_table,I.bk_name from com_redu_test Z '\
    #                'left join stock_informations I '\
    #                'on Z.stock_id = I.stock_id '\
    #                'where redu_5 >= 10000 and trade_date = "2021-04-02" order redu_5 DESC '
    cursor.execute(sql)
    res = cursor.fetchall()
    # res = [('000617','中油资本',111,'7','test'),] * 100
    # id_list = []
    # for tup in res:
    #     id_list.append(tup[0])
    # print('id:',id_list)
    return res
def sel_stock_k_date(res,table,date_e = None,date_s = '2020-08-01'):
    data_list = []
    h_tab_dic ={}
    stock_info = {}
    for stock in res:
        # print('stock:',stock)
        id = stock[1]
        h_tab = stock[4]
        # print('id:',id)
        if h_tab == None:
            continue
        if h_tab not in h_tab_dic:
            h_tab_dic[h_tab] = [id]
        else:
            h_tab_dic[h_tab].append(id)
        info = str(stock[0]) +' '+ str(stock[2]) +' ' + str(stock[3])+' ' + str(stock[5])
        tag = stock[0]
        stock_info[id] = (info,tag)
    rows_list = []
    for h_tab in h_tab_dic:
        if len(h_tab_dic[h_tab]) == 0:
            continue
        elif len(h_tab_dic[h_tab]) == 1:
            h_tab_dic[h_tab].append('fill')
        id_tup = tuple(h_tab_dic[h_tab])
        if date_e == None:
            sql = 'select stock_id,date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,0,0,0,0  '\
                       ' from stock_history_trade{0} where stock_id in {1} and trade_date > "{2}" '.format(h_tab,id_tup,date_s)
        else:
            sql = 'select stock_id,date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,0,0,0,0  '\
                       ' from stock_history_trade{0} where stock_id in {1} and trade_date > "{2}" and trade_date <= "{3}" '.format(h_tab,id_tup,date_s,date_e)
        print('sql:',sql)
        cursor.execute(sql)
        rows = cursor.fetchall()
        # print('rows:',rows)
        rows_list.extend(list(rows))
        print('rows_list:',rows_list)
    stcok_dict = {}
    for tup in rows_list:
        if tup[0] not in stcok_dict:
            stcok_dict[tup[0]] = [list(tup[1:])]
        else:
            stcok_dict[tup[0]].append(list(tup[1:]))
    print('stcok_dict_len:',len(stcok_dict))
    for stock in stcok_dict:
        stcok_data = stcok_dict[stock]
        info =  stock_info[stock][0]
        tag =  stock_info[stock][1]
        rows_list = [table,tag,info,stcok_data]
        # data_list = {table,code(t_code | id),info(id,name,grade),[data]}
        data_list.append(rows_list)
    print('data_list:',data_list)
    return data_list
        # print('row:',rows[i])
        # print(rows)
def del_stock(key,value):
    reason_type = re.findall('\D+', key)[1]
    print('reason_type:',key,reason_type)
    code = re.findall('\d+', key)[0]
    if reason_type == 'zhuang':
        sql = "UPDATE com_zhuang SET monitor = 0,reason = '{0}' where stock_id = '{1}'".format(value,code)
    elif reason_type == 'xiaoboxin':
        sql = "UPDATE remen_xiaoboxin SET monitor = 0,reason = '{0}' where trade_code = '{1}'".format(value, code)
    elif reason_type == 'remen_five':
        sql = "UPDATE com_redu_test SET monitor = 0,reason = '{0}' where trade_code = '{1}'".format(value, code)
    elif reason_type == 'monitor':
        sql = "UPDATE monitor SET monitor = 0,reason = '{0}' where trade_code = '{1}'".format(value, code)
    print('sql:',sql)
    cursor.execute(sql)
def hello(request):
    return HttpResponse('hello world!')
@csrf_exempt
def runoob(request):
    context = {}
    context['data'] = []
    if request.method == "POST":
        # put_reson = json.loads(request.body)
        print('req:',request.POST)
        remen_xiaoboxin_param_dict = {}
        zhuang_param_dict = {}
        remen_5_param_dict = {}
        for key in request.POST:
            print('value:',key,request.POST[key])
            # del_stock(stock_info=key, reson=request.POST[key], db_field='stock_id', db_table='com_zhuang')
            #display处理
            if key[0:6] == 'reason':
                del_stock(key, request.POST[key])
            #respone处理
            elif key == 'monitor_input':
                sql = 'select  Z.trade_code,Z.stock_id,Z.stock_name,Z.grade,I.h_table,I.bk_name,Z.trade_code from monitor Z '\
                           'inner join stock_informations I '\
                           'on Z.stock_id = I.stock_id '\
                           'where monitor = 1 and trade_date ="{0}"  '.format(request.POST[key])
                res = sel_stock_list(cursor,sql)
                print('res:',res)
                data_list = sel_stock_k_date(res,table='monitor')
                print('data_list_len:', len(data_list))
                context['data'] = data_list  # [['2015-10-16',18.4,18.58,18.33,18.79,67.00,1,0.04,0.11,0.09],['2015-10-19',18.56,18.25,18.19,18.56,55.00,0,-0.00,0.08,0.09]]
                # context['hello'] = 'hello world!'
                # return render(request,'echarts_value_g.html',context)
                # return render(request, 'test_page.html', context)
            elif key in ('remen_xiaoboxin_B_input_date_s','remen_xiaoboxin_B_input_date_e','remen_xiaoboxin_B_today_input',
                         'remen_xiaoboxin_B_input_grade_s','remen_xiaoboxin_B_input_grade_e'):
                remen_xiaoboxin_param_dict[key] = request.POST[key]
                
            elif key in ('zhuang_input_date_s', 'zhuang_input_date_e',
                             'zhuang_input_grade_s', 'zhuang_input_grade_e'):
                zhuang_param_dict[key] = request.POST[key]
                
            elif key in ('remen_5_date_s','remen_5_date_e','remen_5_today_input',
                         'remen_5_grade_s','remen_5_grade_e'):
                remen_5_param_dict[key] = request.POST[key]
        if len(remen_xiaoboxin_param_dict) != 0:
            sql = 'select distinct Z.trade_code,Z.stock_id,Z.stock_name,Z.grade,I.h_table,I.bk_name,Z.trade_code from remen_xiaoboxin Z '\
                           'left join stock_informations I '\
                           'on Z.stock_id = I.stock_id '\
                           'where monitor = 1 and grade >= "{0}" and grade <"{1}" and trade_date ="{2}" order by grade DESC'.format(remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_grade_s'],
                                                                                         remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_grade_e'],remen_xiaoboxin_param_dict['remen_xiaoboxin_B_today_input'])
            res = sel_stock_list(cursor, sql)
            data_list = sel_stock_k_date(res,table='xiaoboxin',date_s=remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_date_s'],
                                         date_e=remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_date_e'])
            context['data'] = data_list
        elif len(zhuang_param_dict) != 0:
            sql = 'select distinct Z.stock_id,Z.stock_id,Z.stock_name,Z.zhuang_grade,I.h_table,I.bk_name from com_zhuang Z '\
                           'left join stock_informations I '\
                           'on Z.stock_id = I.stock_id '\
                           'where monitor = 1 and zhuang_grade >= "{0}" and zhuang_grade <"{1}" order by zhuang_grade DESC'.format(zhuang_param_dict['zhuang_input_grade_s'],
                                                                                         zhuang_param_dict['zhuang_input_grade_e'])
            res = sel_stock_list(cursor, sql)
            data_list = sel_stock_k_date(res,table='zhuang',date_s=zhuang_param_dict['zhuang_input_date_s'],date_e=zhuang_param_dict['zhuang_input_date_e'])
            context['data'] = data_list
        elif len(remen_5_param_dict) != 0:
            sql = 'select distinct Z.trade_code,Z.stock_id,Z.stock_name,Z.redu_5,I.h_table,I.bk_name from com_redu_test Z '\
                           'left join stock_informations I '\
                           'on Z.stock_id = I.stock_id '\
                           'where monitor = 1 and redu_5 >= "{0}" and redu_5 <"{1}" and trade_date ="{2}" order by redu_5 DESC'.format(remen_5_param_dict['remen_5_grade_s'],
                                                                                         remen_5_param_dict['remen_5_grade_e'],remen_5_param_dict['remen_5_today_input'])
            res = sel_stock_list(cursor, sql)
            data_list = sel_stock_k_date(res,table='remen_five',date_s=remen_5_param_dict['remen_5_date_s'],date_e=remen_5_param_dict['remen_5_date_e'])
            context['data'] = data_list
        # reason = request.POST['reason']
        # re_res = re.findall('.*?reson(.*?)=(.*?)',bytes(request.body,encoding = "utf-8").decode())
        # print('re_res:',re_res)
    return render(request,'echarts_value_g.html',context)
    # return render(request, 'test_page.html', context)
# def receive(request):
#     if request.POST:
#         print(request.POST.body)
#         reason = request.POST['reason']
#         # cursor.execute(up)
